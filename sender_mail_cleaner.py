#!/usr/bin/env python3
"""
Gmail Cleaner Pro - Профессиональный инструмент для управления почтой
Версия: 2.1
Автор: Улучшенная версия (Исправлено: Синтаксис и Отступы)
"""

import os
import time
import json
import logging
import re
import argparse
import webbrowser
import math
import random
import base64
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Generator
from email.utils import parsedate_to_datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# === Опциональные зависимости ===
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from dateutil import parser as dateutil_parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

try:
    from fuzzywuzzy import fuzz
    FUZZYWUZZY_AVAILABLE = True
except ImportError:
    FUZZYWUZZY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import pyperclip
    PYPERCLIP_AVAILABLE = True
except ImportError:
    PYPERCLIP_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

# === Конфигурация по умолчанию ===
DEFAULT_CONFIG = {
    'scopes': ['https://www.googleapis.com/auth/gmail.modify'],
    'batch_size': 100,
    'batch_delete_size': 1000,
    'delay_between_requests': 0.1,
    'sleep_after_list': 0.2,
    'credentials_file': 'credentials.json',
    'token_file': 'token.json',
    'log_file': 'gmail_cleaner_pro.log',
    'backup_dir': 'email_backups',
    'stats_dir': 'stats',
    'subject_similarity_threshold': 85,
    'max_retries': 5,
    'initial_backoff': 1.0,
    'max_backoff': 60.0,
    'dry_run': False,
    'auto_backup': True,
    'use_batch_delete': True,
}


def load_config(config_file: str = 'gmail_cleaner_config.json') -> dict:
    """Загружает конфигурацию из файла или возвращает дефолтную."""
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                custom_config = json.load(f)
                config = DEFAULT_CONFIG.copy()
                config.update(custom_config)
                return config
        except Exception as e:
            print(f"⚠️ Ошибка загрузки конфига: {e}. Используем дефолтные настройки.")
    return DEFAULT_CONFIG.copy()


def setup_logging(log_file: str):
    """Настройка логирования."""
    os.makedirs(os.path.dirname(log_file) if os.path.dirname(log_file) else '.', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
def display_top_senders(df: 'pd.DataFrame'):
    """Выводит топ-10 самых активных отправителей."""
    if df is None or df.empty:
        return
        
    print("\n--- ТОП-10 САМЫХ АКТИВНЫХ ОТПРАВИТЕЛЕЙ (из sender_counts.csv) ---")
    top_senders = df.head(10)
    
    # Расчет ширины для красивого вывода
    max_len = top_senders['sender_email'].str.len().max() if not top_senders.empty else 40
    
    for index, row in top_senders.iterrows():
        # Выводим индекс + 1 для нумерации
        print(f"{index+1:2}. {row['sender_email']:<{max_len}} | Писем: {row['message_count']}")
    print("-----------------------------------------------------------------")


class GmailCleanerPro:
    """Профессиональный менеджер для чистки Gmail."""
    
    def __init__(self, config: dict = None):
        self.config = load_config()
        if config:
            self.config.update(config)
            
        setup_logging(self.config['log_file'])
        self.logger = logging.getLogger(__name__)
        self.service = None
        self.stats = {
            'found': 0,
            'trashed': 0,
            'errors': 0,
            'skipped': 0,
            'backed_up': 0
        }
        self.messages_cache: List[Tuple[datetime, str, str]] = []

    # ==================== АУТЕНТИФИКАЦИЯ ====================
    
    def authenticate_gmail_api(self):
        """Аутентификация в Gmail API."""
        creds = None
        
        if os.path.exists(self.config['token_file']):
            try:
                creds = Credentials.from_authorized_user_file(
                    self.config['token_file'], 
                    self.config['scopes']
                )
            except Exception as e:
                self.logger.warning(f"Ошибка загрузки токена: {e}")
                os.remove(self.config['token_file'])
                creds = None

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception:
                    creds = None
            
            if not creds:
                if not os.path.exists(self.config['credentials_file']):
                    raise FileNotFoundError(
                        f"❌ Файл {self.config['credentials_file']} не найден!\n"
                        "    Скачайте credentials.json из Google Cloud Console."
                    )
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config['credentials_file'], 
                    self.config['scopes']
                )
                creds = flow.run_local_server(port=0)
            
            with open(self.config['token_file'], 'w', encoding='utf-8') as token:
                token.write(creds.to_json())

        self.service = build('gmail', 'v1', credentials=creds)
        self.logger.info("✅ Успешная аутентификация в Gmail API")

    # ==================== УТИЛИТЫ ====================
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Валидация email адреса."""
        # ИСПРАВЛЕНО: закрывающая кавычка и скобка
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$' 
        return re.match(pattern, email) is not None

    @staticmethod
    def validate_domain(domain: str) -> bool:
        """Валидация домена."""
        # Добавлен вспомогательный метод для проверки домена
        pattern = r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$' 
        return re.match(pattern, domain) is not None

    def exponential_backoff(self, attempt: int) -> float:
        """Экспоненциальная задержка с jitter."""
        base_delay = self.config['initial_backoff'] * (2 ** attempt)
        delay = min(base_delay, self.config['max_backoff'])
        jitter = delay * 0.2 * (random.random() - 0.5)
        return delay + jitter

    def parse_email_date(self, date_str: str) -> datetime:
        """Парсинг даты из заголовка письма."""
        if not date_str:
            return datetime.min.replace(tzinfo=timezone.utc)
        
        try:
            dt = parsedate_to_datetime(date_str)
            if dt and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
        
        if DATEUTIL_AVAILABLE:
            try:
                dt = dateutil_parser.parse(date_str, fuzzy=True)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        
        return datetime.min.replace(tzinfo=timezone.utc)

    def clean_subject(self, subject: str) -> str:
        """Очистка темы письма от префиксов."""
        cleaned = re.sub(r'^(re|fwd|fw|aw):\s*', '', subject, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s*[\(\[]\d+[\)\]]\s*$', '', cleaned)
        return cleaned.strip().lower()

    # ==================== ПОИСК ПИСЕМ ====================
    
    def build_search_query(self, sender_email=None, days_ago=None, 
                           additional_query=None, min_size_mb=None) -> Optional[str]:
        """Построение поискового запроса."""
        parts = []
        
        if sender_email:
            sender_email = sender_email.strip()
            
            if self.validate_email(sender_email):
                parts.append(f"from:{sender_email}")
                self.logger.info(f"🔍 Поиск писем от: {sender_email}")
                
            elif sender_email.startswith('@'):
                domain = sender_email[1:]
                if self.validate_domain(domain):
                    parts.append(f"from:{sender_email}")
                    self.logger.info(f"🔍 Поиск писем от домена: {sender_email}")
                    print(f"\n✅ Поиск всех писем от домена {sender_email}")
                else:
                    self.logger.error(f"❌ Невалидный домен: {domain}")
                    print(f"\n❌ ОШИБКА: '{domain}' не является валидным доменом!")
                    return None
                
            elif self.validate_domain(sender_email):
                parts.append(f"from:@{sender_email}")
                self.logger.info(f"🔍 Поиск писем от домена: @{sender_email}")
                print(f"\n✅ Автоопределение: ищу все письма от домена @{sender_email}")
                
            else:
                self.logger.error(f"❌ Невалидный email/домен: {sender_email}")
                print(f"\n❌ ОШИБКА: '{sender_email}' не является валидным email или доменом!")
                print("\n    Правильные форматы:")
                print("    ✅ noreply@fuib.com    (полный email)")
                print("    ✅ fuib.com            (домен - найдет ВСЕ письма от @fuib.com)")
                print("    ✅ @fuib.com           (домен с @)\n")
                return None
        
        if days_ago:
            date_str = (datetime.now() - timedelta(days=days_ago)).strftime('%Y/%m/%d')
            parts.append(f"before:{date_str}")
        
        if min_size_mb:
            size_bytes = int(min_size_mb * 1024 * 1024)
            parts.append(f"larger:{size_bytes}")
        
        if additional_query:
            parts.append(additional_query)
        
        query = " ".join(parts)
        
        if not query.strip():
            self.logger.warning("⚠️ Пустой поисковый запрос")
            print("\n⚠️ Не указаны критерии поиска. Укажите хотя бы один параметр.")
            return None
            
        return query

    def get_message_metadata_with_retry(self, msg_id: str) -> Optional[Tuple[datetime, str, str]]:
        """Получение метаданных письма с retry."""
        retry_count = 0
        
        while retry_count < self.config['max_retries']:
            try:
                response = self.service.users().messages().get(
                    userId='me', 
                    id=msg_id, 
                    format='metadata', 
                    metadataHeaders=['Subject', 'Date', 'From']
                ).execute()
                
                headers = response.get('payload', {}).get('headers', [])
                subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), "Без темы")
                date_str = next((h['value'] for h in headers if h['name'].lower() == 'date'), None)
                date_obj = self.parse_email_date(date_str)
                
                return (date_obj, response['id'], subject)
                
            except HttpError as e:
                retry_count += 1
                if e.resp.status in [429, 403, 500, 503]:
                    backoff_time = self.exponential_backoff(retry_count)
                    self.logger.warning(f"⏳ Rate limit. Ожидание {backoff_time:.1f}s...")
                    time.sleep(backoff_time)
                else:
                    self.stats['errors'] += 1
                    self.logger.error(f"❌ HTTP ошибка: {e}")
                    return None
            except Exception as e:
                self.stats['errors'] += 1
                self.logger.error(f"❌ Ошибка получения метаданных: {e}")
                return None
            
        return None

    def find_emails_by_criteria(self, query: str) -> List[Tuple[datetime, str, str]]:
        """Поиск писем по критериям."""
        self.logger.info(f"🔍 Поиск писем: {query}")
        self.messages_cache.clear()
        
        # Получаем все ID
        all_message_ids = self._fetch_all_message_ids(query)
        
        if not all_message_ids:
            self.logger.info("📭 Письма не найдены.")
            return []
        
        self.logger.info(f"📊 Найдено {len(all_message_ids)} писем. Загрузка метаданных...")
        
        # Загружаем метаданные
        progress_bar = tqdm(
            total=len(all_message_ids), 
            desc="Загрузка метаданных", 
            unit="писем"
        ) if TQDM_AVAILABLE else None
        
        for msg_id in all_message_ids:
            result = self.get_message_metadata_with_retry(msg_id)
            if result:
                self.messages_cache.append(result)
            
            if progress_bar:
                progress_bar.update(1)
            
            time.sleep(self.config['delay_between_requests'])
        
        if progress_bar:
            progress_bar.close()
        
        # Фильтруем и сортируем
        valid_messages = [
            (dt, mid, subj) 
            for dt, mid, subj in self.messages_cache 
            if dt != datetime.min.replace(tzinfo=timezone.utc)
        ]
        valid_messages.sort(key=lambda x: x[0])
        
        self.messages_cache = valid_messages
        self.stats['found'] = len(self.messages_cache)
        
        return self.messages_cache

    def _fetch_all_message_ids(self, query: str) -> List[str]:
        """Получение всех ID писем по запросу."""
        all_ids = []
        page_token = None
        
        while True:
            try:
                response = self.service.users().messages().list(
                    userId='me',
                    q=f"{query} in:anywhere -in:trash",
                    maxResults=self.config['batch_size'],
                    pageToken=page_token
                ).execute()
                
                messages = response.get('messages', [])
                if not messages:
                    break
                
                all_ids.extend([msg['id'] for msg in messages])
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
                
                time.sleep(self.config['sleep_after_list'])
                
            except HttpError as e:
                self.logger.error(f"❌ Ошибка при получении списка: {e}")
                break
        
        return all_ids

    # ==================== КЛАСТЕРИЗАЦИЯ ====================
    
    def group_by_similar_subjects(self, messages: List[Tuple[datetime, str, str]]) -> List[Tuple[datetime, str, str]]:
        """Группировка писем по похожим темам."""
        if not FUZZYWUZZY_AVAILABLE:
            self.logger.warning("⚠️ fuzzywuzzy не установлен. Пропуск кластеризации.")
            return messages
        
        threshold = self.config['subject_similarity_threshold']
        processed = [(self.clean_subject(subj), dt, mid, subj) for dt, mid, subj in messages]
        
        clusters = []
        unclustered = list(processed)
        
        while unclustered:
            ref_clean, ref_dt, ref_mid, ref_subj = unclustered.pop(0)
            current_cluster = [(ref_dt, ref_mid, ref_subj)]
            
            to_remove = []
            for item in unclustered:
                test_clean, test_dt, test_mid, test_subj = item
                if fuzz.ratio(ref_clean, test_clean) >= threshold:
                    current_cluster.append((test_dt, test_mid, test_subj))
                    to_remove.append(item)
            
            for item in to_remove:
                unclustered.remove(item)
            
            clusters.append(current_cluster)
        
        # Сортировка кластеров по размеру
        clusters.sort(key=len, reverse=True)
        
        # Объединение кластеров в один список
        reordered = []
        for cluster in clusters:
            cluster.sort(key=lambda x: x[0])
            reordered.extend(cluster)
        
        return reordered

    # ==================== BACKUP ====================
    
    def export_emails_before_delete(self, messages_info: List[Tuple[datetime, str, str]]) -> Optional[str]:
        """Экспорт писем в JSON перед удалением."""
        if not messages_info or not self.config['auto_backup']:
            return None
        
        os.makedirs(self.config['backup_dir'], exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(self.config['backup_dir'], f'backup_{timestamp}.json')
        
        self.logger.info(f"💾 Создание backup: {backup_file}")
        
        backup_data = []
        progress_bar = tqdm(
            messages_info, 
            desc="Backup", 
            unit="писем"
        ) if TQDM_AVAILABLE else messages_info
        
        for dt, msg_id, subj in progress_bar:
            try:
                msg = self.service.users().messages().get(
                    userId='me', 
                    id=msg_id, 
                    format='full'
                ).execute()
                backup_data.append({
                    'id': msg_id,
                    'date': dt.isoformat(),
                    'subject': subj,
                    'full_data': msg
                })
                self.stats['backed_up'] += 1
            except Exception as e:
                self.logger.error(f"❌ Ошибка backup для {msg_id}: {e}")
        
        if TQDM_AVAILABLE:
            progress_bar.close()
        
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"✅ Backup сохранен: {backup_file} ({len(backup_data)} писем)")
        return backup_file

    # ==================== УДАЛЕНИЕ ====================
    
    def trash_emails_batch(self, messages_info: List[Tuple[datetime, str, str]]):
        """Массовое удаление писем батчами (быстрее в 10 раз!)."""
        if not messages_info:
            return
        
        total = len(messages_info)
        batch_size = self.config['batch_delete_size']
        
        self.logger.info(f"🗑️ Перемещение {total} писем в корзину (batch режим)...")
        
        def batch_callback(request_id, response, exception):
            if exception:
                self.stats['errors'] += 1
                self.logger.error(f"❌ Ошибка: {exception}")
            else:
                # В батче нет ответа, который бы сказал, сколько успешно удалено. 
                # Считаем, что все письма в запросе успешны, если нет общей ошибки
                pass
        
        trashed_count_in_batch = 0
        
        progress_bar = tqdm(
            total=total, 
            desc="Перемещение в Корзину (Batch)", 
            unit="писем"
        ) if TQDM_AVAILABLE else None

        for i in range(0, total, batch_size):
            chunk = messages_info[i:i + batch_size]
            batch = self.service.new_batch_http_request(callback=batch_callback)
            
            for dt, msg_id, subj in chunk:
                batch.add(
                    self.service.users().messages().trash(userId='me', id=msg_id)
                )
                
            try:
                batch.execute()
                trashed_count_in_batch += len(chunk)
                if progress_bar:
                    progress_bar.update(len(chunk))
                time.sleep(self.config['delay_between_requests'])
            except Exception as e:
                self.logger.error(f"❌ Ошибка batch операции: {e}")
                
        if progress_bar:
            progress_bar.close()

        self.stats['trashed'] += trashed_count_in_batch
        self.logger.info(f"✅ Batch завершен. Перемещено: {trashed_count_in_batch}")


    def trash_emails_interactive(self, messages_info: List[Tuple[datetime, str, str]]):
        """Интерактивное удаление с превью и подтверждением."""
        if not messages_info:
            return
        
        PREVIEW_SIZE = 20
        total = len(messages_info)
        
        print(f"\n📊 Найдено {total} писем. Показываю первые {min(total, PREVIEW_SIZE)}:\n")
        print("-" * 80)
        
        for i, (dt, msg_id, subj) in enumerate(messages_info[:PREVIEW_SIZE]):
            date_str = dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')
            subj_short = subj[:55] + '...' if len(subj) > 55 else subj
            print(f"[{i+1:02}] {date_str} | {subj_short}")
        
        print("-" * 80)
        
        if self.config['dry_run']:
            print("⚠️ РЕЖИМ DRY-RUN: письма НЕ будут удалены.")
            self.stats['skipped'] = total
            return
        
        # Подтверждение
        action = input(f"\nУдалить {total} писем? (y/n/c[ustom]/b[ackup]): ").strip().lower()
        
        messages_to_delete = []
        skipped_count = 0
        
        if action == 'y':
            messages_to_delete = messages_info
        
        elif action == 'c':
            keep_str = input(f"Сколько последних писем оставить? (0-{total}): ").strip()
            try:
                keep_count = int(keep_str)
                keep_count = max(0, min(keep_count, total))
            except ValueError:
                keep_count = 0
            
            messages_to_delete = messages_info[:-keep_count] if keep_count > 0 else messages_info
            skipped_count = keep_count
            
            if not messages_to_delete:
                print("✅ Все письма оставлены.")
                self.stats['skipped'] = total
                return
            
            print(f"📝 Будет удалено: {len(messages_to_delete)}, оставлено: {skipped_count}")
        
        elif action == 'b':
            # Создать backup и удалить
            self.export_emails_before_delete(messages_info)
            messages_to_delete = messages_info
        
        else:
            print("❌ Операция отменена.")
            self.stats['skipped'] = total
            return
        
        # Удаление
        if self.config['use_batch_delete'] and len(messages_to_delete) >= 10:
            self.trash_emails_batch(messages_to_delete)
        else:
            # Если мало писем, удаляем по одному, чтобы прогресс-бар был точнее
            initial_trashed = self.stats['trashed']
            progress_bar = tqdm(
                messages_to_delete, 
                desc="Удаление", 
                unit="писем"
            ) if TQDM_AVAILABLE else messages_to_delete
            
            for dt, msg_id, subj in progress_bar:
                self.trash_single_message_with_retry(msg_id)
                time.sleep(self.config['delay_between_requests'])
            
            if TQDM_AVAILABLE:
                progress_bar.close()
            
            # Обновляем статистику trashed в этом конкретном вызове
            trashed_in_call = self.stats['trashed'] - initial_trashed
            self.stats['trashed'] = initial_trashed + trashed_in_call
            
            
        self.stats['skipped'] += skipped_count
        print(f"✅ Готово! Удалено: {len(messages_to_delete)}, пропущено: {skipped_count}")

    def trash_single_message_with_retry(self, msg_id: str) -> bool:
        """Удаление одного письма с retry."""
        retry_count = 0
        
        while retry_count < self.config['max_retries']:
            try:
                self.service.users().messages().trash(userId='me', id=msg_id).execute()
                self.stats['trashed'] += 1
                return True
            except HttpError as e:
                retry_count += 1
                if e.resp.status in [429, 403]:
                    backoff_time = self.exponential_backoff(retry_count)
                    time.sleep(backoff_time)
                else:
                    self.stats['errors'] += 1
                    return False
            except Exception:
                self.stats['errors'] += 1
                return False
        
        return False

    # ==================== ОТПИСКА ОТ РАССЫЛОК ====================
    
    def find_unsubscribe_link(self, msg_id: str) -> Optional[str]:
        """Поиск ссылки для отписки в письме."""
        try:
            msg = self.service.users().messages().get(
                userId='me', 
                id=msg_id, 
                format='full'
            ).execute()
            
            # Проверяем заголовок List-Unsubscribe
            headers = msg.get('payload', {}).get('headers', [])
            for h in headers:
                if h.get('name', '').lower() == 'list-unsubscribe':
                    link = h['value']
                    # Ищем HTTP/HTTPS ссылку в угловых скобках
                    match = re.search(r'<(https?://[^>]+)>', link)
                    if match:
                        return match.group(1)
            
            # Ищем в теле письма
            body = self._get_email_body(msg)
            patterns = [
                r'(https?://[^\s]+unsubscribe[^\s"<>]*)',
                r'(https?://[^\s]+opt-out[^\s"<>]*)',
                r'(https?://[^\s]+remove[^\s"<>]*)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, body, re.IGNORECASE)
                if match:
                    url = match.group(1).rstrip('.,;)')
                    return url
        
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска ссылки отписки: {e}")
            
        return None

    def _get_email_body(self, msg: dict) -> str:
        """Извлечение текста из тела письма."""
        
        def decode_part(part):
            body_data = part.get('body', {}).get('data', '')
            if body_data:
                return base64.urlsafe_b64decode(body_data).decode('utf-8', errors='ignore')
            return ''
        
        payload = msg.get('payload', {})
        
        # Простое письмо
        if 'body' in payload and payload['body'].get('data'):
            return decode_part(payload)
        
        # Multipart
        if 'parts' in payload:
            text = ''
            for part in payload['parts']:
                if part.get('mimeType') == 'text/plain':
                    text += decode_part(part)
                elif part.get('mimeType') == 'text/html':
                    text += decode_part(part)
            return text
        
        return ''

    def interactive_unsubscribe(self, sender_email: str):
        """Интерактивная отписка от рассылки."""
        print(f"\n📧 Поиск писем от {sender_email}...")
        
        query = f"from:{sender_email}"
        messages = self.find_emails_by_criteria(query)
        
        if not messages:
            print(f"❌ Письма от {sender_email} не найдены.")
            return
        
        # Берем последнее письмо
        latest = messages[-1]
        dt, msg_id, subject = latest
        
        print(f"\n📬 Последнее письмо:")
        print(f"    Тема: {subject}")
        print(f"    Дата: {dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n🔍 Поиск ссылки для отписки...")
        unsubscribe_link = self.find_unsubscribe_link(msg_id)
        
        if not unsubscribe_link:
            print("⚠️ Ссылка для отписки не найдена.")
            print("     Попробуйте отписаться вручную из письма.")
            return
        
        print(f"\n✅ Найдена ссылка:")
        print(f"    {unsubscribe_link}\n")
        
        # Варианты действий
        print("Выберите действие:")
        print("  1 - Открыть в браузере (рекомендуется)")
        print("  2 - Скопировать ссылку")
        if REQUESTS_AVAILABLE:
            print("  3 - Автоматический переход (осторожно!)")
        print("  0 - Отмена")
        
        choice = input("\nВыбор: ").strip()
        
        if choice == '1':
            print("🌐 Открываю браузер...")
            webbrowser.open(unsubscribe_link)
            print("✅ Завершите отписку в браузере.")
        
        elif choice == '2':
            if PYPERCLIP_AVAILABLE:
                try:
                    pyperclip.copy(unsubscribe_link)
                    print("✅ Ссылка скопирована в буфер обмена!")
                except Exception as e:
                    print(f"⚠️ Ошибка копирования: {e}")
                    print(f"    Скопируйте вручную: {unsubscribe_link}")
            else:
                print("⚠️ pyperclip не установлен.")
                print(f"    Скопируйте вручную: {unsubscribe_link}")
        
        elif choice == '3' and REQUESTS_AVAILABLE:
            confirm = input("⚠️ ВНИМАНИЕ! Автоматический переход может быть небезопасен.\n"
                              "    Продолжить? (yes/no): ").strip().lower()
            
            if confirm == 'yes':
                try:
                    response = requests.get(unsubscribe_link, timeout=10)
                    if response.status_code == 200:
                        print("✅ Запрос отправлен. Проверьте почту.")
                    else:
                        print(f"⚠️ Статус: {response.status_code}. Возможно, нужно подтверждение.")
                except Exception as e:
                    print(f"❌ Ошибка: {e}")
            else:
                print("Отменено.")
        
        else:
            print("❌ Отменено.")

    # ==================== СТАТИСТИКА ====================
    
    def load_sender_counts(self, filename: str = 'sender_counts.csv') -> Optional['pd.DataFrame']:
        """Загрузка статистики отправителей из CSV."""
        if not PANDAS_AVAILABLE:
            return None
        
        if not os.path.exists(filename):
            return None
        
        try:
            df = pd.read_csv(filename)
            
            if 'sender_email' not in df.columns or 'message_count' not in df.columns:
                self.logger.error("❌ CSV должен содержать 'sender_email' и 'message_count'")
                return None
            
            df['message_count'] = pd.to_numeric(df['message_count'], errors='coerce').fillna(0).astype(int)
            df = df.sort_values('message_count', ascending=False).reset_index(drop=True)
            
            self.logger.info(f"✅ Загружена статистика: {len(df)} отправителей")
            return df
        
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения CSV: {e}")
            return None

    def save_stats(self):
        """Сохранение статистики работы."""
        os.makedirs(self.config['stats_dir'], exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stats_file = os.path.join(self.config['stats_dir'], f'stats_{timestamp}.json')
        
        detailed_stats = {
            'timestamp': datetime.now().isoformat(),
            'summary': self.stats,
            'config': self.config,
            'dry_run': self.config['dry_run']
        }
        
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_stats, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"📊 Статистика сохранена: {stats_file}")

    def display_stats(self):
        """Вывод финальной статистики."""
        print("\n" + "="*50)
        print("📊 СТАТИСТИКА РАБОТЫ")
        print("="*50)
        print(f"🔍 Найдено писем:     {self.stats['found']}")
        print(f"🗑️ Удалено:            {self.stats['trashed']}")
        print(f"⏭️ Пропущено:          {self.stats['skipped']}")
        print(f"💾 Backup создан:      {self.stats['backed_up']}")
        print(f"❌ Ошибок:             {self.stats['errors']}")
        print("="*50)


# ==================== CLI ====================

def parse_arguments():
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description='Gmail Cleaner Pro - Профессиональный инструмент управления почтой'
    )
    parser.add_argument('--sender', type=str, help='Email отправителя')
    parser.add_argument('--days', type=int, help='Письма старше N дней')
    parser.add_argument('--dry-run', action='store_true', help='Режим без удаления')
    parser.add_argument('--batch-size', type=int, default=100, help='Размер батча')
    parser.add_argument('--no-backup', action='store_true', help='Отключить auto-backup')
    parser.add_argument('--unsubscribe', type=str, help='Отписаться от рассылки (email)')
    
    return parser.parse_args()


# ==================== MAIN ====================

def main():
    """Главная функция программы."""
    
    args = parse_arguments()
    
    config_override = {
        'dry_run': args.dry_run,
        'batch_size': args.batch_size,
        'auto_backup': not args.no_backup,
        'use_batch_delete': True
    }
    
    cleaner = GmailCleanerPro(config=config_override)
    
    print("="*60)
    print("      📧 GMAIL CLEANER PRO v2.1 - Полностью рабочий скрипт 🚀")
    print("="*60)
    
    try:
        cleaner.authenticate_gmail_api()
    except Exception as e:
        print(f"❌ Ошибка аутентификации: {e}")
        return
    
    # --- БЛОК ОБРАБОТКИ CLI АРГУМЕНТОВ ---
    if args.unsubscribe:
        cleaner.interactive_unsubscribe(args.unsubscribe)
        return
    
    if args.sender:
        print("💡 Запущен одноразовый режим очистки через аргументы CLI.")
        query = cleaner.build_search_query(
            sender_email=args.sender,
            days_ago=args.days,
            additional_query=None,
            min_size_mb=None
        )
        if query:
            messages_info = cleaner.find_emails_by_criteria(query)
            if messages_info:
                messages_info = cleaner.group_by_similar_subjects(messages_info)
                cleaner.trash_emails_interactive(messages_info)
        cleaner.display_stats()
        cleaner.save_stats()
        return

    # --- БЛОК ИНТЕРАКТИВНОГО ЦИКЛА (Как вы просили) ---
    
    sender_df = cleaner.load_sender_counts() if PANDAS_AVAILABLE else None
    
    if sender_df is not None:
        display_top_senders(sender_df)
    
    while True:
        print("\n" + "="*70)
        print("          ✨ ГОТОВНОСТЬ К ЧИСТКЕ (Введите 'q' для выхода)")
        print("="*70)
        
        sender_input = input("Email/домен отправителя для чистки (или 'q', 'top', 'unsub'): ").strip()
        
        if sender_input.lower() in ['q', 'exit', 'quit']:
            print("👋 Завершение работы программы.")
            break
            
        if sender_input.lower() == 'top':
            if sender_df is not None:
                display_top_senders(sender_df)
            else:
                print("⚠️ Статистика отправителей не загружена. Установите Pandas и создайте sender_counts.csv.")
            continue
            
        if sender_input.lower() == 'unsub':
            unsub_email = input("Введите email для отписки (e.g., mail@newsletter.com): ").strip()
            if unsub_email:
                cleaner.interactive_unsubscribe(unsub_email)
            continue
            
        if not sender_input:
            print("⚠️ Не указан отправитель. Попробуйте снова.")
            continue
            
        days = input("Письма старше N дней (пусто — все): ").strip()
        days_ago = int(days) if days.isdigit() else None
        
        size = input("Письма крупнее N МБ (пусто — все): ").strip()
        min_size_mb = float(size) if size.replace('.', '', 1).isdigit() else None
        
        additional_query = input("Дополнительно (например, 'subject:ads' или пусто): ").strip()

        query = cleaner.build_search_query(
            sender_email=sender_input, 
            days_ago=days_ago, 
            additional_query=additional_query,
            min_size_mb=min_size_mb
        )
        
        if query is None:
            continue
            
        messages_info = cleaner.find_emails_by_criteria(query)

        if messages_info:
            messages_info = cleaner.group_by_similar_subjects(messages_info)
            cleaner.trash_emails_interactive(messages_info)
        else:
            print("📭 Письма по заданным критериям не найдены.")

    # Вывод и сохранение общей статистики после выхода из цикла
    cleaner.display_stats()
    cleaner.save_stats()
    
if __name__ == '__main__':
    main()