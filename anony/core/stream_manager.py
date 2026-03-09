# Copyright (c) 2025 AnonymousX1025
# Licensed under the MIT License.
# This file is part of AnonXMusic

"""
StreamManager — segment-based streaming engine
===============================================

How it works
------------
Instead of downloading the full song before playing, we:

  1. Extract the direct audio URL from YouTube (fast, ~1 s).
  2. Use ffmpeg to cut that URL into fixed-length segments (default 30 s).
  3. Play segment[0] immediately — while it plays, segment[1] is downloading.
  4. When segment[0] finishes, segment[1] is ready → play it, start segment[2].
  5. Repeat until the song ends, then clean up all temp files.

Segment files are stored as:
  downloads/seg_{video_id}_{index}.webm

A segment is "ready" as soon as ffmpeg writes it; pytgcalls streams it like
any normal local file, so quality is identical to a full download.
"""

import asyncio
import os
import shutil
from pathlib import Path

from anony import logger


# Length of each audio segment in seconds
SEGMENT_DURATION = 30


class SongStream:
    """Manages all segments for a single song."""

    def __init__(self, video_id: str, stream_url: str, total_sec: int):
        self.video_id = video_id
        self.stream_url = stream_url
        self.total_sec = total_sec

        # Calculate how many segments this song needs
        self.total_segments = max(1, -(-total_sec // SEGMENT_DURATION))  # ceiling div

        # segment index → file path (set once the segment file is ready)
        self.segments: dict[int, str] = {}

        # segment index → asyncio.Event (set when that segment file is ready)
        self.ready: dict[int, asyncio.Event] = {
            i: asyncio.Event() for i in range(self.total_segments)
        }

        self.current_index = 0
        self.cancelled = False
        self._download_task: asyncio.Task | None = None

    def seg_path(self, index: int) -> str:
        return f"downloads/seg_{self.video_id}_{index}.webm"

    def cleanup(self):
        """Delete all segment files for this song."""
        for i in range(self.total_segments):
            path = self.seg_path(i)
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

    def cancel(self):
        self.cancelled = True
        if self._download_task and not self._download_task.done():
            self._download_task.cancel()


class StreamManager:
    """
    Global manager — one SongStream per active chat.
    Called by calls.py to get the next segment file path.
    """

    def __init__(self):
        # chat_id → SongStream
        self._streams: dict[int, SongStream] = {}

    # ------------------------------------------------------------------
    # Public API used by calls.py
    # ------------------------------------------------------------------

    async def prepare(self, chat_id: int, video_id: str, stream_url: str, total_sec: int) -> str | None:
        """
        Start segmenting a song and return the path to segment 0.
        Segment 0 will be ready in ~2-3 s (just the first 30 s to fetch).
        """
        # Cancel any existing stream for this chat
        self.stop(chat_id)

        os.makedirs("downloads", exist_ok=True)
        song = SongStream(video_id, stream_url, total_sec)
        self._streams[chat_id] = song

        # Start downloading all segments in the background
        song._download_task = asyncio.create_task(
            self._download_all_segments(song)
        )

        # Wait for segment 0 to be ready (should be ~2-3 s)
        try:
            await asyncio.wait_for(song.ready[0].wait(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("Segment 0 timed out for %s", video_id)
            self.stop(chat_id)
            return None

        if song.cancelled:
            return None

        song.current_index = 0
        return song.seg_path(0)

    async def next_segment(self, chat_id: int) -> str | None:
        """
        Called when the current segment finishes playing.
        Waits for the next segment to be ready, then returns its path.
        Returns None when the song is fully played or was stopped.
        """
        song = self._streams.get(chat_id)
        if not song or song.cancelled:
            return None

        next_index = song.current_index + 1

        if next_index >= song.total_segments:
            # Song is done
            self.stop(chat_id)
            return None

        # Wait for the next segment (usually already ready since it downloaded
        # while the previous segment was playing)
        try:
            await asyncio.wait_for(song.ready[next_index].wait(), timeout=20)
        except asyncio.TimeoutError:
            logger.warning("Segment %d timed out for %s", next_index, song.video_id)
            self.stop(chat_id)
            return None

        if song.cancelled:
            return None

        song.current_index = next_index
        return song.seg_path(next_index)

    def has_more(self, chat_id: int) -> bool:
        """True if there are more segments left to play."""
        song = self._streams.get(chat_id)
        if not song or song.cancelled:
            return False
        return song.current_index + 1 < song.total_segments

    def stop(self, chat_id: int) -> None:
        """Cancel and clean up the stream for a chat."""
        song = self._streams.pop(chat_id, None)
        if song:
            song.cancel()
            song.cleanup()

    def is_active(self, chat_id: int) -> bool:
        return chat_id in self._streams

    # ------------------------------------------------------------------
    # Internal: download all segments sequentially in background
    # ------------------------------------------------------------------

    async def _download_all_segments(self, song: SongStream) -> None:
        for i in range(song.total_segments):
            if song.cancelled:
                break

            start_sec = i * SEGMENT_DURATION
            path = song.seg_path(i)

            success = await self._fetch_segment(
                stream_url=song.stream_url,
                output_path=path,
                start_sec=start_sec,
                duration_sec=SEGMENT_DURATION,
            )

            if success and not song.cancelled:
                song.segments[i] = path
                song.ready[i].set()
                logger.info(
                    "[StreamManager] Segment %d/%d ready for %s",
                    i + 1, song.total_segments, song.video_id,
                )
            else:
                # Mark as ready with empty path so waiters don't hang;
                # calls.py will handle the missing file gracefully.
                song.ready[i].set()
                if not song.cancelled:
                    logger.warning(
                        "[StreamManager] Segment %d failed for %s", i, song.video_id
                    )
                break  # Don't continue if a segment fails

    @staticmethod
    async def _fetch_segment(
        stream_url: str,
        output_path: str,
        start_sec: int,
        duration_sec: int,
    ) -> bool:
        """
        Use ffmpeg to cut [start_sec, start_sec+duration_sec] from the
        direct YouTube audio URL and save as a webm/opus file.
        """
        cmd = [
            "ffmpeg",
            "-hide_banner", "-loglevel", "error",
            "-ss", str(start_sec),
            "-i", stream_url,
            "-t", str(duration_sec),
            "-c:a", "libopus",
            "-b:a", "128k",
            "-vn",
            "-f", "webm",
            "-y",              # overwrite if exists
            output_path,
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                err = stderr.decode(errors="ignore").strip()
                logger.warning("[StreamManager] ffmpeg error: %s", err)
                return False
            return Path(output_path).exists() and Path(output_path).stat().st_size > 0
        except asyncio.CancelledError:
            # Clean up partial file
            try:
                os.remove(output_path)
            except FileNotFoundError:
                pass
            return False
        except Exception as ex:
            logger.warning("[StreamManager] _fetch_segment exception: %s", ex)
            return False
