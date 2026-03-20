"""
PWS Market Scheduler
Daily automatic discovery of new markets.

Runs an asyncio loop that waits until a configured time (default: 00:05 UTC)
and then calls MarketManager.refresh() to switch to the new day's markets.

No external dependencies — uses pure asyncio.sleep for scheduling.
"""
import asyncio
import logging
from datetime import datetime, date, timezone, timedelta
from typing import Optional

from .manager import MarketManager

log = logging.getLogger("pws.scheduler")


class MarketScheduler:
    """
    Daily market refresh scheduler.
    
    Usage:
        manager = MarketManager(cities=["ankara"])
        scheduler = MarketScheduler(manager, refresh_hour_utc=0, refresh_minute_utc=5)
        
        await manager.start()         # Initial discovery + stream
        await scheduler.start()       # Blocks — runs daily loop
    """
    
    def __init__(self, manager: MarketManager, 
                 refresh_hour_utc: int = 0, refresh_minute_utc: int = 5):
        self.manager = manager
        self.refresh_hour = refresh_hour_utc
        self.refresh_minute = refresh_minute_utc
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start the daily scheduler loop."""
        self._running = True
        log.info(f"⏰ Scheduler started — daily refresh at {self.refresh_hour:02d}:{self.refresh_minute:02d} UTC")
        
        while self._running:
            try:
                # Calculate seconds until next refresh
                wait_seconds = self._seconds_until_next_refresh()
                next_time = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
                
                log.info(f"⏳ Next refresh at {next_time.strftime('%Y-%m-%d %H:%M UTC')} ({wait_seconds/3600:.1f}h)")
                
                # Wait
                await asyncio.sleep(wait_seconds)
                
                if not self._running:
                    break
                
                # Refresh to tomorrow's markets
                tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
                log.info(f"🔄 Scheduled refresh triggered for {tomorrow}")
                
                await self.manager.refresh(tomorrow)
                
                # Small delay to avoid duplicate triggers
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Scheduler error: {e}")
                await asyncio.sleep(300)  # Wait 5 min on error
    
    async def run_now(self):
        """Manually trigger a refresh right now."""
        today = date.today().strftime("%Y-%m-%d")
        log.info(f"🔄 Manual refresh triggered for {today}")
        await self.manager.refresh(today)
    
    async def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
        log.info("⏰ Scheduler stopped")
    
    def start_background(self) -> asyncio.Task:
        """Start scheduler as background task (non-blocking)."""
        self._task = asyncio.create_task(self.start())
        return self._task
    
    def _seconds_until_next_refresh(self) -> float:
        """Calculate seconds from now until the next refresh time."""
        now = datetime.now(timezone.utc)
        
        # Target time today
        target = now.replace(
            hour=self.refresh_hour,
            minute=self.refresh_minute,
            second=0,
            microsecond=0,
        )
        
        # If we've passed today's target, schedule for tomorrow
        if now >= target:
            target += timedelta(days=1)
        
        return (target - now).total_seconds()
