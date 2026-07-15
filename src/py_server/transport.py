"""Транспорты для OneCClient.

Транспорт отвечает ТОЛЬКО за доставку строки JSON-RPC запроса в 1С и получение
строки ответа. Формирование JSON-RPC и разбор результата — в OneCClient.
Контракт зеркалит 1С-сторону (mcp_Диспетчер.ОбработатьСтроку: строка → строка).
"""

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Optional, Protocol, runtime_checkable
import httpx


logger = logging.getLogger(__name__)


@runtime_checkable
class Transport(Protocol):
    """Контракт транспорта: строка запроса → строка ответа."""

    async def send(self, request_body: str) -> str:
        """Доставить тело JSON-RPC запроса и вернуть тело ответа.

        Для notifications (запрос без id) 1С отвечает пустым телом — возвращается "".
        """
        ...

    async def check_health(self) -> bool:
        """Проверить доступность 1С данным транспортом."""
        ...

    async def close(self) -> None:
        """Освободить ресурсы транспорта."""
        ...


class HttpTransport:
    """HTTP-транспорт к HTTP-сервису 1С (прямой режим или через прокси).

    Переносит без изменений логику работы с httpx из прежнего OneCClient.
    """

    def __init__(self, base_url: str, username: Optional[str], password: Optional[str],
                 service_root: str = "mcp", unlock_code: Optional[str] = None):
        """Инициализация транспорта.

        Args:
            base_url: Базовый URL 1С (например, http://localhost/base)
            username: Имя пользователя
            password: Пароль
            service_root: Корневой URL HTTP-сервиса (по умолчанию "mcp")
            unlock_code: Код разрешения (unlock code) для входа при блокировке
                начала сеансов. Передаётся в каждый запрос как query-параметр uc.
        """
        self.base_url = base_url.rstrip('/')
        self.service_root = service_root.strip('/')
        self.auth = httpx.BasicAuth(username, password)

        # Код разрешения для обхода блокировки начала сеансов (?uc=<код>)
        self.request_params = {"uc": unlock_code} if unlock_code else None

        # Используем метод для создания клиента
        self.client = self._create_client()

        # Формируем базовый URL для HTTP-сервиса
        self.service_base_url = f"{self.base_url}/hs/{self.service_root}"
        logger.debug(f"Базовый URL HTTP-сервиса: {self.service_base_url}")

    def _create_client(self) -> httpx.AsyncClient:
        """Создание нового HTTP-клиента."""
        return httpx.AsyncClient(
            auth=self.auth,
            timeout=30.0,
            headers={"Content-Type": "application/json"}
        )

    async def _ensure_client(self):
        """Проверка состояния клиента и восстановление при необходимости."""
        if self.client.is_closed:
            logger.warning("HTTP-клиент был закрыт, выполняется восстановление...")
            self.client = self._create_client()
            logger.info("HTTP-клиент успешно восстановлен")

    async def send(self, request_body: str) -> str:
        """Отправить тело JSON-RPC запроса POST-ом на /rpc, вернуть тело ответа."""
        await self._ensure_client()

        url = f"{self.service_base_url}/rpc"

        response = await self.client.post(
            url,
            content=request_body.encode("utf-8"),
            params=self.request_params,
        )
        response.raise_for_status()

        return response.text

    async def check_health(self) -> bool:
        """Проверить состояние HTTP-сервиса 1С.

        Returns:
            True, если сервис доступен и здоров, иначе вызывает исключение.
        """
        import json

        try:
            # Проверяем и восстанавливаем клиент при необходимости
            await self._ensure_client()

            url = f"{self.service_base_url}/health"
            logger.debug(f"Запрос состояния здоровья: {url}")

            response = await self.client.get(url, params=self.request_params)
            response.raise_for_status()

            # Проверяем JSON ответ от 1C healthGET
            try:
                response_json = response.json()
                if response_json.get("status") == "ok":
                    logger.debug("Сервис 1С доступен и здоров (статус OK).")
                    return True
                else:
                    logger.warning(f"1C health check вернул неожиданный статус: {response_json}")
                    raise httpx.HTTPStatusError(f"1C service reported not healthy: {response_json}", request=response.request, response=response)
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга JSON ответа health-check 1С: {response.text}")
                raise httpx.HTTPStatusError(f"Invalid JSON response from 1C health check: {e}", request=response.request, response=response)

        except httpx.HTTPError as e:
            logger.error(f"Ошибка HTTP при проверке состояния 1С: {type(e).__name__}: {str(e) or repr(e)}")
            raise

    async def close(self) -> None:
        """Закрыть HTTP-клиент."""
        await self.client.aclose()


class FileTransport:
    """Файловый транспорт: обмен с 1С-агентом через папку (для тестов без веб-сервера).

    Протокол (зеркалит форму mcp_УправлениеСервером):
    - запрос кладётся в `<exchange_dir>/in/<uuid>.json`;
    - 1С-агент по таймеру забирает `in/*.json`, обрабатывает через
      mcp_Диспетчер.ОбработатьСтроку и пишет ответ в `out/<то же имя>`,
      а запрос из `in/` удаляет;
    - Python опрашивает `out/<uuid>.json`, читает, удаляет.

    Запрос пишется атомарно (через временное имя + rename), чтобы 1С (маска
    `*.json`) не подхватил недописанный файл. Ответ 1С локально пишет НЕ атомарно,
    поэтому при незавершённой записи (JSON ещё не парсится) ждём следующий опрос.
    """

    def __init__(self, exchange_dir: str, poll_interval: float = 0.2, timeout: float = 30.0):
        """Инициализация транспорта.

        Args:
            exchange_dir: Папка обмена; внутри — подпапки in/ и out/.
            poll_interval: Как часто (сек) проверять появление ответа в out/.
            timeout: Сколько (сек) ждать ответ, затем TimeoutError.
        """
        self.exchange_dir = Path(exchange_dir)
        self.in_dir = self.exchange_dir / "in"
        self.out_dir = self.exchange_dir / "out"
        self.poll_interval = poll_interval
        self.timeout = timeout

    def _ensure_dirs(self) -> None:
        """Гарантировать наличие in/ и out/ (создание идемпотентно)."""
        self.in_dir.mkdir(parents=True, exist_ok=True)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    async def send(self, request_body: str) -> str:
        """Положить запрос в in/, дождаться ответа в out/ (под тем же именем)."""
        self._ensure_dirs()

        # Notifications (запрос без id) 1С-агент не отвечает — не ждём ответа.
        # OneCClient всегда шлёт id, ветка на всякий случай.
        try:
            is_notification = "id" not in json.loads(request_body)
        except (json.JSONDecodeError, TypeError):
            is_notification = False

        name = f"{uuid.uuid4().hex}.json"
        req_path = self.in_dir / name
        resp_path = self.out_dir / name
        tmp_path = self.in_dir / f"{name}.tmp"

        # Атомарная запись запроса: пишем во временный файл, затем rename в *.json
        tmp_path.write_text(request_body, encoding="utf-8")
        os.replace(tmp_path, req_path)

        if is_notification:
            return ""

        deadline = time.monotonic() + self.timeout
        while True:
            if resp_path.exists():
                try:
                    # utf-8-sig снимает BOM, который пишет 1С ЗаписьТекста
                    text = resp_path.read_text(encoding="utf-8-sig")
                    json.loads(text)  # проверка, что ответ дописан целиком
                except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                    # ответ ещё пишется — ждём следующий опрос
                    pass
                else:
                    try:
                        resp_path.unlink()
                    except OSError:
                        pass
                    return text

            if time.monotonic() >= deadline:
                # Уберём наш запрос, чтобы не копился, если агент не отвечает
                try:
                    req_path.unlink()
                except OSError:
                    pass
                raise TimeoutError(
                    f"Ответ 1С не получен за {self.timeout} с (файл {resp_path}). "
                    f"Проверьте, что 1С-агент запущен и опрашивает {self.in_dir}."
                )

            await asyncio.sleep(self.poll_interval)

    async def check_health(self) -> bool:
        """Проверить доступность ПАПКИ ОБМЕНА (не самой 1С).

        Файловый транспорт не может проверить, запущен ли 1С-агент, — это выяснится
        таймаутом на первом вызове. Здесь проверяется только доступность каталога.
        """
        try:
            self._ensure_dirs()
            # Проба на запись — каталог in/ действительно пригоден
            probe = self.in_dir / f".health_{uuid.uuid4().hex}.tmp"
            probe.write_text("", encoding="utf-8")
            probe.unlink()
        except OSError as e:
            logger.error(f"Папка обмена недоступна ({self.exchange_dir}): {e}")
            raise
        logger.info(
            f"Файловый транспорт: папка обмена доступна ({self.exchange_dir}: in/, out/). "
            f"Доступность 1С-агента этим НЕ проверяется — выяснится при первом вызове."
        )
        return True

    async def close(self) -> None:
        """Файловому транспорту нечего закрывать."""
        return None
