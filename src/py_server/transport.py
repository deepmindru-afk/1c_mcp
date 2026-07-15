"""Транспорты для OneCClient.

Транспорт отвечает ТОЛЬКО за доставку строки JSON-RPC запроса в 1С и получение
строки ответа. Формирование JSON-RPC и разбор результата — в OneCClient.
Контракт зеркалит 1С-сторону (mcp_Диспетчер.ОбработатьСтроку: строка → строка).
"""

import logging
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
