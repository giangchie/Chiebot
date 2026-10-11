import asyncio
import discord
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from discord import ui, ButtonStyle
from discord.ext import commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ========== CẤU HÌNH ==========
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
HANOI_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DATA_FILE = Path(__file__).resolve().with_name("events_data.json")
first_ready = True  # Chặn on_ready chạy nhiều lần
_last_sent = {}    # Chống gửi tin nhắn trùng lặp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("discord_bot")

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True


class HealthcheckBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.health_server = None

    async def setup_hook(self):
        await super().setup_hook()
        port_value = os.getenv("PORT")
        if not port_value:
            logger.info("PORT chưa được đặt; bỏ qua health-check trong workspace.")
            return
        try:
            port = int(port_value)
            if not 1 <= port <= 65535:
                raise ValueError("PORT phải nằm trong khoảng 1-65535.")
        except ValueError as exc:
            raise RuntimeError(f"PORT không hợp lệ: {port_value!r}") from exc
        self.health_server = await asyncio.start_server(
            self._handle_health_request,
            host="0.0.0.0",
            port=port,
        )
        logger.info("Health-check đang lắng nghe tại 0.0.0.0:%s", port)

    async def _handle_health_request(self, reader, writer):
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            if not request_line:
                return
            for _ in range(100):
                header = await asyncio.wait_for(reader.readline(), timeout=5)
                if header in (b"\r\n", b"\n", b""):
                    break
            else:
                writer.write(
                    b"HTTP/1.1 431 Request Header Fields Too Large\r\n"
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
                await writer.drain()
                return
            request_parts = request_line.decode("latin-1").strip().split()
            method = request_parts[0] if request_parts else ""
            if method in {"GET", "HEAD"}:
                body = b"ok\n"
                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain; charset=utf-8\r\n"
                    + f"Content-Length: {len(body)}\r\n".encode("ascii")
                    + b"Connection: close\r\n\r\n"
                )
                if method != "HEAD":
                    writer.write(body)
            else:
                writer.write(
                    b"HTTP/1.1 405 Method Not Allowed\r\n"
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
            await writer.drain()
        except (asyncio.TimeoutError, ConnectionError, OSError, ValueError):
            logger.debug("Kết nối health-check đóng trước khi nhận đủ yêu cầu.")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def close(self):
        if self.health_server is not None:
            self.health_server.close()
            await self.health_server.wait_closed()
            self.health_server = None
        await super().close()


bot = HealthcheckBot(command_prefix="", intents=intents, help_command=None)
scheduler = AsyncIOScheduler(
    timezone=HANOI_TZ,
    job_defaults={"misfire_grace_time": 300, "coalesce": True, "max_instances": 1},
)
active_events = {}
registered_view_ids = set()


# ========== LƯU & TẢI DỮ LIỆU ==========
def load_data():
    try:
        data_file = DATA_FILE.open("r", encoding="utf-8")
    except FileNotFoundError:
        active_events.clear()
        return
    try:
        with data_file as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Không thể đọc dữ liệu sự kiện: {DATA_FILE}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"Dữ liệu sự kiện phải là một JSON object: {DATA_FILE}")

    loaded_events = {}
    needs_migration = False
    for name, value in raw.items():
        try:
            if not isinstance(name, str) or not name or not isinstance(value, dict):
                raise ValueError("tên hoặc cấu trúc sự kiện không hợp lệ")
            cycle_seconds = value["cycle_seconds"]
            if (
                isinstance(cycle_seconds, bool)
                or not isinstance(cycle_seconds, int)
                or cycle_seconds <= 0
            ):
                raise ValueError("chu kỳ phải là số nguyên dương")
            next_time_str = value["next_time_str"]
            if not isinstance(next_time_str, str):
                raise ValueError("thời gian sự kiện không hợp lệ")
            try:
                next_time = datetime.fromisoformat(next_time_str)
            except ValueError:
                next_time = datetime.strptime(
                    next_time_str, "%H:%M:%S %d/%m/%Y"
                )
                needs_migration = True
            if next_time.tzinfo is None or next_time.utcoffset() is None:
                next_time = next_time.astimezone(HANOI_TZ)
                needs_migration = True
            else:
                next_time = next_time.astimezone(HANOI_TZ)
            channel_id = str(value["channel_id"])
            if not channel_id.isdigit():
                raise ValueError("channel_id không hợp lệ")
            loaded_events[name] = {
                "cycle_delta": timedelta(seconds=cycle_seconds),
                "next_time": next_time,
                "channel_id": channel_id,
            }
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                f"Dữ liệu sự kiện {name!r} không hợp lệ trong {DATA_FILE}"
            ) from exc
    active_events.clear()
    active_events.update(loaded_events)
    if needs_migration:
        save_data()
    logger.info("Đã tải %s sự kiện từ %s", len(active_events), DATA_FILE)


def save_data():
    data = {}
    for name, v in active_events.items():
        data[name] = {
            "cycle_seconds": int(v["cycle_delta"].total_seconds()),
            "next_time_str": v["next_time"].isoformat(timespec="seconds"),
            "channel_id": v["channel_id"]
        }
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=DATA_FILE.parent,
            prefix=f"{DATA_FILE.name}.",
            suffix=".tmp",
            delete=False,
        ) as f:
            temporary_path = f.name
            json.dump(data, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary_path, DATA_FILE)
    except Exception:
        if temporary_path and os.path.exists(temporary_path):
            try:
                os.unlink(temporary_path)
            except OSError:
                logger.warning("Không xóa được file tạm %s", temporary_path)
        raise


def parse_time_str(s):
    if not re.fullmatch(r"[0-9]+:[0-5][0-9]:[0-5][0-9]", s):
        raise ValueError("Chu kỳ phải theo định dạng HH:MM:SS.")
    h, m, seconds = map(int, s.split(":"))
    try:
        cycle = timedelta(hours=h, minutes=m, seconds=seconds)
    except OverflowError as exc:
        raise ValueError("Chu kỳ quá lớn.") from exc
    if cycle <= timedelta(0):
        raise ValueError("Chu kỳ phải lớn hơn 00:00:00.")
    return cycle


def parse_clock_time(value):
    parsed = datetime.strptime(value, "%H:%M:%S").time()
    if parsed.strftime("%H:%M:%S") != value:
        raise ValueError("Thời gian phải theo định dạng HH:MM:SS.")
    return parsed


def next_occurrence(previous_time, cycle_delta, now=None):
    if cycle_delta <= timedelta(0):
        raise ValueError("Chu kỳ sự kiện phải lớn hơn 0.")
    now = now or datetime.now(HANOI_TZ)
    previous_time = previous_time.astimezone(HANOI_TZ)
    if previous_time > now:
        return previous_time
    missed_cycles = (now - previous_time) // cycle_delta + 1
    return previous_time + cycle_delta * missed_cycles


# ========== NÚT OK ==========
class OkView(ui.View):
    def __init__(self, event_name):
        super().__init__(timeout=None)
        self.event_name = event_name
        custom_id = "event-ok:" + hashlib.sha256(
            event_name.encode("utf-8")
        ).hexdigest()
        button = ui.Button(
            emoji="😆",
            style=ButtonStyle.primary,
            custom_id=custom_id,
        )
        button.callback = self.ok_btn
        self.add_item(button)

    async def ok_btn(self, interaction: discord.Interaction):
        event = active_events.get(self.event_name)
        if event is None:
            await interaction.response.send_message(
                "Sự kiện này đã bị xóa.", ephemeral=True
            )
            return
        await interaction.response.defer()
        previous_next = event["next_time"]
        new_next = datetime.now(HANOI_TZ) + event["cycle_delta"]
        event["next_time"] = new_next
        try:
            save_data()
        except Exception:
            event["next_time"] = previous_next
            logger.exception("Không thể lưu lần xác nhận sự kiện %r", self.event_name)
            await interaction.followup.send(
                "❌ Không lưu được lịch mới; hãy thử lại sau.", ephemeral=True
            )
            return
        try:
            schedule_event(self.event_name)
        except Exception:
            logger.exception("Không thể cập nhật lịch sự kiện %r", self.event_name)
        try:
            await interaction.message.edit(view=None)
        except discord.HTTPException:
            logger.warning("Không thể gỡ nút xác nhận của %r", self.event_name)
        await interaction.followup.send(
            f"✅ {self.event_name} xác nhận! Lần sau: {new_next.strftime('%H:%M:%S')}"
        )


# ========== LÊN LỊCH SỰ KIỆN ==========
def remove_event_jobs(event_name):
    # Xóa job theo id chính xác
    for suffix in ["_3m", "_1m", "_start"]:
        job_id = f"{event_name}{suffix}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            logger.info("Đã xóa job cũ: %s", job_id)
    # Phòng ngừa: quét toàn bộ job liên quan
    for job in scheduler.get_jobs():
        if job.id.startswith(event_name + "_"):
            scheduler.remove_job(job.id)
            logger.info("Đã dọn dẹp job thừa: %s", job.id)


async def send_event_message(channel_id, event_name, content, **kwargs):
    global _last_sent

    # Chống gửi tin nhắn trùng lặp trong vòng 10 giây
    key = (str(channel_id), event_name, content[:30])
    now_ts = time.time()
    if key in _last_sent and now_ts - _last_sent[key] < 10:
        logger.info("🛑 Bỏ qua tin nhắn trùng: %s → %s", event_name, content[:20])
        return False
    _last_sent[key] = now_ts

    # Dọn dẹp cache cũ
    _last_sent = {k: v for k, v in _last_sent.items() if now_ts - v < 60}

    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except discord.HTTPException as exc:
            logger.warning(
                "Không lấy được kênh cho sự kiện %r (channel %s): %s",
                event_name, channel_id, exc,
            )
            return False
    try:
        await channel.send(content, **kwargs)
        return True
    except discord.HTTPException as exc:
        logger.warning("Không gửi được thông báo sự kiện %r: %s", event_name, exc)
        return False


def schedule_event(event_name):
    remove_event_jobs(event_name)
    data = active_events.get(event_name)
    if data is None:
        return
    nt = data["next_time"]
    cycle_delta = data["cycle_delta"]
    if cycle_delta <= timedelta(0):
        raise ValueError(f"Chu kỳ sự kiện {event_name!r} không hợp lệ.")
    now = datetime.now(HANOI_TZ)
    if nt <= now:
        nt = next_occurrence(nt, cycle_delta, now)
        data["next_time"] = nt
        try:
            save_data()
        except Exception:
            logger.exception("Không thể lưu giờ chạy tiếp theo của %r", event_name)
    ch_id = int(data["channel_id"])

    async def remind_3m():
        await send_event_message(ch_id, event_name, f"⏰ {event_name} — còn 3 phút nữa!")

    async def remind_1m():
        await send_event_message(ch_id, event_name, f"⏰ {event_name} — còn 1 phút nữa!")

    async def event_trigger():
        current = active_events.get(event_name)
        if current is None:
            return
        scheduled_time = current["next_time"]
        await send_event_message(
            ch_id,
            event_name,
            f"📢 {event_name} — BẮT ĐẦU!",
            view=OkView(event_name),
        )
        current = active_events.get(event_name)
        if current is None:
            return
        if current["next_time"] == scheduled_time:
            current["next_time"] = next_occurrence(
                scheduled_time,
                current["cycle_delta"],
                datetime.now(HANOI_TZ),
            )
            try:
                save_data()
            except Exception:
                logger.exception("Không thể lưu chu kỳ kế tiếp của %r", event_name)
        try:
            schedule_event(event_name)
        except Exception:
            logger.exception("Không thể lên lịch lại sự kiện %r", event_name)

    t3 = nt - timedelta(minutes=3)
    t1 = nt - timedelta(minutes=1)
    if t3 > now: scheduler.add_job(remind_3m, "date", run_date=t3, id=f"{event_name}_3m")
    if t1 > now: scheduler.add_job(remind_1m, "date", run_date=t1, id=f"{event_name}_1m")
    if nt > now: scheduler.add_job(event_trigger, "date", run_date=nt, id=f"{event_name}_start")


# ========== BOT SẴN SÀNG ==========
@bot.event
async def on_ready():
    global first_ready
    if not first_ready:
        logger.info("on_ready chạy lần trước rồi, bỏ qua...")
        return
    first_ready = True

    logger.info("Đăng nhập với tên: %s", bot.user)

    # Dừng & xóa TẤT CẢ job cũ trước khi khởi tạo mới
    if scheduler.running:
        scheduler.remove_all_jobs()
    else:
        scheduler.start()

    registered_view_ids.clear()

    for name in list(active_events.keys()):
        view_id = "event-ok:" + hashlib.sha256(name.encode("utf-8")).hexdigest()
        if view_id not in registered_view_ids:
            bot.add_view(OkView(name))
            registered_view_ids.add(view_id)
        try:
            schedule_event(name)
        except Exception:
            logger.exception("Không thể khôi phục lịch sự kiện %r", name)

    logger.info("✅ Hệ thống sự kiện sẵn sàng — Đã chống lỗi gửi trùng tin nhắn")


# ========== XỬ LÝ TẤT CẢ LỆNH ==========
@bot.event
async def on_message(msg):
    if msg.author.bot: return
    text = msg.content.strip()

    # === %list ===
    if text.lower() == "%list":
        if not active_events:
            embed = discord.Embed(
                title="📅 Sự kiện sắp tới",
                description="Hiện chưa có sự kiện nào được lên lịch.",
                color=discord.Color.from_rgb(116, 132, 150),
            )
            embed.set_footer(text="Giờ Hà Nội · UTC+7")
            try:
                await msg.channel.send(embed=embed)
            except discord.Forbidden:
                await msg.channel.send(
                    "Bot cần quyền **Embed Links** để hiển thị bảng sự kiện."
                )
            return

        sorted_list = sorted(active_events.items(), key=lambda x: x[1]["next_time"])
        for page_start in range(0, len(sorted_list), 20):
            page = sorted_list[page_start:page_start + 20]
            embed = discord.Embed(
                title="📅 Sự kiện sắp tới",
                description="Danh sách được sắp xếp theo giờ diễn ra.",
                color=discord.Color.from_rgb(116, 132, 150),
            )
            for index, (name, data) in enumerate(page, start=page_start + 1):
                next_time = data["next_time"]
                embed.add_field(
                    name="\u200b",
                    value=(
                        f"{index:02d} · {name[:220]}"
                        f"\n⏰ {next_time.strftime('%H:%M:%S  %d/%m/%Y')}"
                    ),
                    inline=False,
                )
            embed.set_footer(
                text=f"{len(sorted_list)} sự kiện · Giờ Hà Nội (UTC+7)"
            )
            try:
                await msg.channel.send(embed=embed)
            except discord.Forbidden:
                await msg.channel.send(
                    "Bot cần quyền **Embed Links** để hiển thị bảng sự kiện."
                )
                return
        return

    # === %time_<TênSựKiện> HH:MM:SS ===
    if text.lower().startswith("%time_"):
        parts = text.split(maxsplit=1)
        if len(parts) != 2:
            await msg.channel.send(
                "❌ Cú pháp: `%time_TenSuKien HH:MM:SS` "
                "(ví dụ: `%time_Bossevent2 22:05:00`)."
            )
            return
        requested_name = parts[0][len("%time_"):]
        if not requested_name:
            await msg.channel.send(
                "❌ Cú pháp: `%time_TenSuKien HH:MM:SS` "
                "(ví dụ: `%time_Bossevent2 22:05:00`)."
            )
            return
        event_name = requested_name if requested_name in active_events else next(
            (
                name for name in active_events
                if name.casefold() == requested_name.casefold()
            ),
            None,
        )
        if event_name is None:
            await msg.channel.send(
                f"❌ Không tìm thấy sự kiện **{requested_name}**. "
                "Dùng `%list` để xem tên sự kiện."
            )
            return
        try:
            start_clock = parse_clock_time(parts[1])
        except ValueError:
            await msg.channel.send(
                "❌ Giờ không hợp lệ. Hãy dùng định dạng `HH:MM:SS`, "
                "ví dụ: `%time_Bossevent2 22:05:00`."
            )
            return
        now = datetime.now(HANOI_TZ)
        next_time = now.replace(
            hour=start_clock.hour,
            minute=start_clock.minute,
            second=start_clock.second,
            microsecond=0,
        )
        if next_time <= now:
            next_time += timedelta(days=1)
        previous_next = active_events[event_name]["next_time"]
        active_events[event_name]["next_time"] = next_time
        try:
            save_data()
        except Exception:
            active_events[event_name]["next_time"] = previous_next
            logger.exception("Không thể lưu giờ mới của sự kiện %r", event_name)
            await msg.channel.send("❌ Không lưu được giờ mới; hãy thử lại sau.")
            return
        try:
            schedule_event(event_name)
        except Exception:
            logger.exception("Không thể cập nhật lịch sự kiện %r", event_name)
            await msg.channel.send(
                "⚠️ Đã lưu giờ mới nhưng chưa cập nhật được bộ hẹn giờ. "
                "Hãy khởi động lại bot."
            )
            return
        await msg.channel.send(
            f"✅ Đã đổi giờ bắt đầu **{event_name}** sang "
            f"**{next_time.strftime('%H:%M:%S %d/%m/%Y')}** "
            "(giờ Hà Nội). Chu kỳ lặp vẫn giữ nguyên."
        )
        return

    # === %del TênSựKiện ===
    if text.lower().startswith("%del "):
        name = text[5:].strip()
        if name not in active_events:
            await msg.channel.send(f"❌ Không tìm thấy sự kiện: {name}")
            return
        remove_event_jobs(name)
        removed_event = active_events.pop(name)
        try:
            save_data()
        except Exception:
            active_events[name] = removed_event
            logger.exception("Không thể lưu việc xóa sự kiện %r", name)
            try:
                schedule_event(name)
            except Exception:
                logger.exception("Không thể khôi phục lịch sự kiện %r", name)
            await msg.channel.send("❌ Không lưu được thay đổi; sự kiện vẫn được giữ.")
            return
        await msg.channel.send(f"✅ Đã xóa: {name}")
        return

    # === %TênSựKiện HH:MM:SS ===
    if text.startswith("%") and " " in text:
        parts = text[1:].split(maxsplit=1)
        if len(parts) != 2 or not parts[0]:
            await msg.channel.send("❌ Cú pháp: `%TenSuKien HH:MM:SS`")
            return
        event_name, time_str = parts[0], parts[1]
        try:
            cycle = parse_time_str(time_str)
        except ValueError:
            await msg.channel.send(
                "❌ Chu kỳ phải lớn hơn `00:00:00` và theo định dạng "
                "`HH:MM:SS`, ví dụ: `%Sukien1 00:06:00`."
            )
            return
        next_time = datetime.now(HANOI_TZ) + cycle
        previous_event = active_events.get(event_name)
        active_events[event_name] = {
            "cycle_delta": cycle,
            "next_time": next_time,
            "channel_id": str(msg.channel.id)
        }
        try:
            save_data()
        except Exception:
            if previous_event is None:
                active_events.pop(event_name, None)
            else:
                active_events[event_name] = previous_event
            logger.exception("Không thể lưu sự kiện %r", event_name)
            await msg.channel.send("❌ Không lưu được sự kiện; hãy thử lại sau.")
            return
        try:
            schedule_event(event_name)
        except Exception:
            logger.exception("Không thể lên lịch sự kiện %r", event_name)
            await msg.channel.send(
                "⚠️ Đã lưu sự kiện nhưng chưa lên lịch được. "
                "Hãy khởi động lại bot."
            )
            return
        await msg.channel.send(
            f"🤪 {event_name} — Đã tạo! Lần đầu: {next_time.strftime('%H:%M:%S')}"
        )
        return


def main():
    if not TOKEN:
        raise RuntimeError(
            "Thiếu DISCORD_BOT_TOKEN. Hãy thêm token mới trong Replit Secrets."
        )
    load_data()
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
