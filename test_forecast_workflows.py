"""Offline regression tests; no Telegram calls or database connection."""
import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

for key in ("BOT_TOKEN", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_HOST"):
    os.environ.setdefault(key, "offline-test")

import main as bot


class ForecastTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.conn.fetch = AsyncMock(return_value=[])
        self.conn.fetchrow = AsyncMock()
        self.conn.fetchval = AsyncMock()
        self.conn.execute = AsyncMock()
        self.conn.transaction.return_value.__aenter__ = AsyncMock()
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=self.conn)
        self.patches = [patch.object(bot, "db_pool", pool), patch.object(bot, "ADMIN_IDS", [99])]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)
        self.message = SimpleNamespace(from_user=SimpleNamespace(id=7), text="2-1", answer=AsyncMock())
        self.data = dict(forecast_iso_year=2026, forecast_week=38,
                         current_match_index=2, forecast_match_name="A — B",
                         af_year=2026, af_week=38, af_tid=7, af_index=2,
                         af_name="Player", af_match_name="A — B", af_old=None, af_score="2-1")
        self.state = SimpleNamespace(get_data=AsyncMock(return_value=self.data),
                                     get_state=AsyncMock(return_value=bot.ForecastStates.waiting_for_score.state),
                                     clear=AsyncMock(), update_data=AsyncMock(), set_state=AsyncMock())

    def test_schedule_boundaries(self):
        for day, hour, minute, expected in [(15,17,59,False), (15,18,0,True),
                                           (18,20,59,True), (18,21,0,False),
                                           (19,12,0,False), (20,12,0,False)]:
            with self.subTest(day=day, hour=hour, minute=minute):
                with patch.object(bot, "moscow_now", return_value=datetime(2026,9,day,hour,minute,tzinfo=bot.MOSCOW_TZ)):
                    self.assertEqual(bot.is_forecast_open_schedule(), expected)

    async def test_manual_modes_preserved(self):
        for mode, expected in [("open", True), ("closed", False), ("auto", False)]:
            with patch.object(bot, "get_forecast_mode", AsyncMock(return_value=mode)), patch.object(bot, "is_forecast_open_schedule", return_value=False):
                self.assertEqual(await bot.forecast_open_on_connection(self.conn), expected)

    async def test_partial_forecast_can_restart(self):
        with patch.object(bot, "is_forecast_open_effective", AsyncMock(return_value=True)), \
             patch.object(bot, "validate_match_set_1_to_10", AsyncMock(return_value=(True,""))), \
             patch.object(bot, "forecast_set_locked", AsyncMock(return_value=False)), \
             patch.object(bot, "send_next_match", AsyncMock()) as next_match:
            await bot.handle_make_forecast(self.message, self.state)
            next_match.assert_awaited_once()

    async def test_next_missing_match_selected(self):
        self.conn.fetchrow.return_value = {"match_index": 4, "match_name": "C — D"}
        await bot.send_next_match(self.message, self.state)
        self.state.update_data.assert_awaited_once_with(current_match_index=4, forecast_match_name="C — D")
        self.assertIn("NOT EXISTS", self.conn.fetchrow.call_args.args[0])
        self.assertEqual(self.conn.fetchrow.call_args.args[-1], 7)
        self.assertIn("матча 4", self.message.answer.call_args.args[0])

    async def test_all_matches_complete(self):
        self.conn.fetchrow.return_value = None
        with patch.object(bot, "send_main_menu", AsyncMock()):
            await bot.send_next_match(self.message, self.state)
        self.state.clear.assert_awaited_once()
        self.assertIn("Все 10", self.message.answer.call_args.args[0])

    async def test_cancel_keeps_saved_forecasts(self):
        await bot.cancel_action(self.message, self.state)
        self.conn.execute.assert_not_awaited()
        self.assertIn("Внесённые прогнозы сохранены", self.message.answer.call_args.args[0])

    async def test_score_after_deadline_not_saved(self):
        with patch.object(bot, "is_forecast_open_effective", AsyncMock(return_value=False)), patch.object(bot, "send_main_menu", AsyncMock()):
            await bot.process_forecast_score(self.message, self.state)
        self.conn.execute.assert_not_awaited()
        self.conn.fetchval.assert_not_awaited()

    async def check_user_save(self, name, inserted):
        self.conn.fetchval.side_effect = [name, inserted] if name == "A — B" else [name]
        with patch.object(bot, "is_forecast_open_effective", AsyncMock(return_value=True)), \
             patch.object(bot, "forecast_open_on_connection", AsyncMock(return_value=True)), \
             patch.object(bot, "forecast_set_locked", AsyncMock(return_value=False)), \
             patch.object(bot, "current_isoyear_week", return_value=(2026,38)), \
             patch.object(bot, "send_next_match", AsyncMock()):
            await bot.process_forecast_score(self.message, self.state)

    async def test_user_cannot_overwrite_existing_score(self):
        await self.check_user_save("A — B", None)
        sql = self.conn.fetchval.call_args.args[0]
        self.assertIn("DO NOTHING", sql)
        self.assertNotIn("DO UPDATE", sql)
        self.assertIn("только администратор", self.message.answer.call_args.args[0])

    async def test_changed_match_requires_new_answer(self):
        await self.check_user_save("A — C", None)
        self.assertEqual(self.conn.fetchval.await_count, 1)
        self.assertIn("Матч изменён", self.message.answer.call_args.args[0])

    async def test_non_admin_denied_at_every_step(self):
        for handler in (bot.admin_forecast_start, bot.admin_forecast_user, bot.admin_forecast_match,
                        bot.admin_forecast_score, bot.admin_forecast_confirm):
            await handler(self.message, self.state)
        self.conn.execute.assert_not_awaited()
        self.conn.fetchval.assert_not_awaited()

    async def test_admin_confirmation_upserts_target_and_logs(self):
        self.message.from_user.id = 99
        self.message.text = "Да"
        self.conn.fetchval.side_effect = [1, "A — B", None]
        with patch.object(bot, "get_latest_matches_set", AsyncMock(return_value=(2026,38))), \
             patch.object(bot, "forecast_set_locked", AsyncMock(return_value=False)), \
             patch.object(bot, "admin_forecast_show_matches", AsyncMock()), \
             patch.object(bot, "is_forecast_open_effective", AsyncMock(return_value=False)):
            await bot.admin_forecast_confirm(self.message, self.state)
        calls = self.conn.execute.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIn("DO UPDATE", calls[0].args[0])
        self.assertEqual(calls[0].args[1:], (7,2026,38,2,"2-1"))
        self.assertIn("admin_actions", calls[1].args[0])

    async def test_admin_cannot_change_after_results_start(self):
        self.message.from_user.id = 99
        self.message.text = "Да"
        with patch.object(bot, "get_latest_matches_set", AsyncMock(return_value=(2026,38))), \
             patch.object(bot, "forecast_set_locked", AsyncMock(return_value=True)):
            await bot.admin_forecast_confirm(self.message, self.state)
        self.conn.execute.assert_not_awaited()
        self.assertIn("результатов", self.message.answer.call_args.args[0])

    async def test_admin_stale_confirmation_cannot_overwrite(self):
        self.message.from_user.id = 99
        self.message.text = "Да"
        self.conn.fetchval.side_effect = [1, "A — B", "3-0"]
        with patch.object(bot, "get_latest_matches_set", AsyncMock(return_value=(2026,38))), \
             patch.object(bot, "forecast_set_locked", AsyncMock(return_value=False)):
            await bot.admin_forecast_confirm(self.message, self.state)
        self.conn.execute.assert_not_awaited()
        self.assertIn("изменился", self.message.answer.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
