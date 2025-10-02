# --- адаптивная версия с умным throttling ---
from __future__ import print_function
import os, pickle, time, csv, re, json, argparse, logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, Optional, Any, List
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Установите tqdm: pip install tqdm")

CONFIG: Dict[str, Any] = {
    'scopes': ['https://www.googleapis.com/auth/gmail.readonly'],
    'token_file': 'token.pickle',
    'credentials_file': 'credentials.json',
    'result_file': 'sender_counts.csv',
    'initial_delay': 0.05,  # Более агрессивная начальная задержка
    'max_delay': 8.0,       # Уменьшили максимальную задержку
    'backoff_factor': 2.0,  # Более быстрое увеличение при ошибках
    'success_reduction': 0.85, # Более быстрое уменьшение при успехах
    'max_retries': 3,
    'chunk_size': 15,       # Увеличили размер чанка
    'adaptive_throttling': True,
    'turbo_mode': False,    # Режим для очень быстрой обработки
    'success_threshold': 5  # Уменьшение задержки каждые 5 успехов
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AdaptiveThrottler:
    """Адаптивный регулятор скорости запросов"""
    def __init__(self):
        self.current_delay = CONFIG['initial_delay']
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        
    def on_success(self):
        """Вызывается при успешном запросе"""
        self.consecutive_successes += 1
        self.consecutive_failures = 0
        
        # Каждые N успешных запросов уменьшаем задержку
        threshold = CONFIG.get('success_threshold', 5)
        if self.consecutive_successes >= threshold:
            old_delay = self.current_delay
            self.current_delay = max(
                CONFIG['initial_delay'],
                self.current_delay * CONFIG['success_reduction']
            )
            self.consecutive_successes = 0
            if abs(old_delay - self.current_delay) > 0.01:  # Логируем только значимые изменения
                logging.info(f"🚀 Ускорили: {old_delay:.3f}s → {self.current_delay:.3f}s")
    
    def on_rate_limit(self):
        """Вызывается при 429 ошибке"""
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        
        # Увеличиваем задержку
        old_delay = self.current_delay
        self.current_delay = min(
            CONFIG['max_delay'],
            self.current_delay * CONFIG['backoff_factor']
        )
        logging.warning(f"⚠️  429! Замедлили: {old_delay:.3f}s → {self.current_delay:.3f}s")
    
    def wait(self):
        """Ожидание перед следующим запросом"""
        # В турбо-режиме используем минимальные задержки
        if CONFIG.get('turbo_mode', False):
            time.sleep(max(0.01, self.current_delay * 0.5))
        else:
            time.sleep(self.current_delay)
    
    def get_current_delay(self) -> float:
        return self.current_delay

class GmailAnalyzer:
    def __init__(self):
        self.service: Optional[Any] = None
        self.sender_counts: Dict[str,int] = defaultdict(int)
        self.domain_counts: Dict[str,int] = defaultdict(int)
        self.total_processed = 0
        self.total_errors = 0
        self.throttler = AdaptiveThrottler()

    def get_service(self):
        creds = None
        if os.path.exists(CONFIG['token_file']):
            with open(CONFIG['token_file'],'rb') as f:
                creds = pickle.load(f)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CONFIG['credentials_file'], CONFIG['scopes'])
                creds = flow.run_local_server(port=0)
            with open(CONFIG['token_file'],'wb') as f:
                pickle.dump(creds, f)
        self.service = build('gmail','v1',credentials=creds)

    @staticmethod
    def extract_email(sender: str) -> str:
        if not sender:
            return "unknown"
        # Улучшенное регулярное выражение для email
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        m = re.search(pattern, sender, re.IGNORECASE)
        return m.group(0).lower() if m else sender.lower()

    @staticmethod
    def extract_domain(email: str) -> str:
        return email.split('@')[-1].lower() if '@' in email else 'unknown'

    def _process_single_message(self, message_id: str) -> bool:
        """Обработка одного сообщения с повторными попытками"""
        retry_count = 0
        
        while retry_count <= CONFIG['max_retries']:
            try:
                # Ждем перед запросом
                if CONFIG['adaptive_throttling']:
                    self.throttler.wait()
                
                response = self.service.users().messages().get(
                    userId='me', 
                    id=message_id, 
                    format='metadata'
                ).execute()
                
                # Успешный запрос
                self.throttler.on_success()
                self._extract_sender_info(response)
                return True
                
            except HttpError as e:
                if e.resp.status == 429:
                    self.throttler.on_rate_limit()
                    retry_count += 1
                    
                    if retry_count <= CONFIG['max_retries']:
                        # Более умная дополнительная задержка при 429
                        extra_wait = min(20, 2 * retry_count)  # Уменьшили дополнительную паузу
                        logging.warning(f"429 для {message_id}, попытка {retry_count}/{CONFIG['max_retries']}, ждем {extra_wait}s")
                        time.sleep(extra_wait)
                    else:
                        logging.error(f"Исчерпаны попытки для {message_id}")
                        self.total_errors += 1
                        return False
                else:
                    logging.error(f"HTTP ошибка {e.resp.status} для {message_id}: {e}")
                    self.total_errors += 1
                    return False
                    
            except Exception as e:
                logging.error(f"Неожиданная ошибка для {message_id}: {e}")
                self.total_errors += 1
                return False
        
        return False

    def _extract_sender_info(self, response: Dict[str, Any]):
        """Извлечение информации об отправителе"""
        try:
            payload = response.get('payload', {})
            headers = payload.get('headers', [])
            
            sender = None
            for header in headers:
                if header.get('name', '').lower() == 'from':
                    sender = self.extract_email(header.get('value', ''))
                    break
            
            if sender and sender != "unknown":
                self.sender_counts[sender] += 1
                self.domain_counts[self.extract_domain(sender)] += 1
            else:
                self.sender_counts["unknown"] += 1
                self.domain_counts["unknown"] += 1
            
            self.total_processed += 1
            
        except Exception as e:
            logging.error(f"Ошибка извлечения данных отправителя: {e}")
            self.sender_counts["unknown"] += 1
            self.domain_counts["unknown"] += 1
            self.total_processed += 1

    def _process_chunk(self, messages: List[Dict[str, str]], progress_bar=None) -> int:
        """Обработка чанка сообщений"""
        processed = 0
        
        for msg in messages:
            success = self._process_single_message(msg['id'])
            if success:
                processed += 1
            
            if progress_bar:
                progress_bar.update(1)
                # Обновляем описание с текущей задержкой
                current_delay = self.throttler.get_current_delay()
                progress_bar.set_description(f"Обработка (задержка: {current_delay:.2f}s)")
        
        return processed

    def count_emails(self, days_back: Optional[int]=None, query: Optional[str]=None, max_emails: Optional[int]=None):
        if not self.service: 
            self.get_service()
            
        q = ''
        if days_back:
            date_filter = (datetime.now(timezone.utc)-timedelta(days=days_back)).strftime('%Y/%m/%d')
            q += f"after:{date_filter} "
        if query: 
            q += query
        q = q.strip()
        
        logging.info(f"Поиск: {q if q else 'Все письма'}")

        # Получаем список всех сообщений
        all_messages = []
        page_token = None
        
        while True:
            try:
                resp = self.service.users().messages().list(
                    userId='me', q=q,
                    maxResults=1000, pageToken=page_token
                ).execute()
                msgs = resp.get('messages',[])
                if not msgs: 
                    break
                    
                all_messages.extend(msgs)
                
                if max_emails and len(all_messages) >= max_emails:
                    all_messages = all_messages[:max_emails]
                    break
                    
                page_token = resp.get('nextPageToken')
                if not page_token: 
                    break
                    
            except HttpError as e:
                logging.error(f"Ошибка получения списка сообщений: {e}")
                break

        if not all_messages:
            logging.info("Сообщения не найдены")
            return

        total_messages = len(all_messages)
        logging.info(f"Найдено сообщений: {total_messages}")
        logging.info(f"Начальная задержка: {CONFIG['initial_delay']}s")
        
        # Progress bar
        progress_bar = None
        if TQDM_AVAILABLE:
            progress_bar = tqdm(
                total=total_messages, 
                desc=f"Обработка (задержка: {CONFIG['initial_delay']}s)",
                unit="msg"
            )

        # Обрабатываем сообщения чанками
        start_time = time.time()
        processed_chunks = 0
        
        for i in range(0, total_messages, CONFIG['chunk_size']):
            chunk = all_messages[i:i+CONFIG['chunk_size']]
            processed_in_chunk = self._process_chunk(chunk, progress_bar)
            processed_chunks += 1
            
            # Логируем прогресс каждые 5 чанков для лучшего мониторинга
            if processed_chunks % 5 == 0:
                elapsed = time.time() - start_time
                rate = self.total_processed / elapsed if elapsed > 0 else 0
                current_delay = self.throttler.get_current_delay()
                logging.info(f"📊 Чанк {processed_chunks}: {self.total_processed}/{total_messages} сообщений, {rate:.1f} msg/s, задержка: {current_delay:.3f}s")

        if progress_bar: 
            progress_bar.close()

        elapsed_time = time.time() - start_time
        rate = self.total_processed / elapsed_time if elapsed_time > 0 else 0
        
        logging.info(f"=== ЗАВЕРШЕНО ===")
        logging.info(f"Всего обработано: {self.total_processed}")
        logging.info(f"Ошибок: {self.total_errors}")
        logging.info(f"Время: {elapsed_time:.1f}s")
        logging.info(f"Средняя скорость: {rate:.1f} сообщений/сек")
        logging.info(f"Финальная задержка: {self.throttler.get_current_delay():.2f}s")

    def save_csv(self, filename: str = 'sender_counts.csv'):
        with open(filename,'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(['Sender','Count','Domain'])
            for s, c in sorted(self.sender_counts.items(), key=lambda x: x[1], reverse=True):
                w.writerow([s, c, self.extract_domain(s)])
        logging.info(f"CSV сохранен: {filename}")

    def save_json(self, filename: str = 'report.json'):
        report = {
            'total_processed': self.total_processed,
            'total_errors': self.total_errors,
            'unique_senders': len(self.sender_counts),
            'unique_domains': len(self.domain_counts),
            'final_delay': self.throttler.get_current_delay(),
            'top_senders': dict(list(sorted(self.sender_counts.items(), key=lambda x: x[1], reverse=True))[:50]),
            'top_domains': dict(list(sorted(self.domain_counts.items(), key=lambda x: x[1], reverse=True))[:20])
        }
        with open(filename,'w', encoding='utf-8') as f: 
            json.dump(report, f, ensure_ascii=False, indent=2)
        logging.info(f"JSON сохранен: {filename}")

    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики обработки"""
        return {
            'total_emails': self.total_processed,
            'total_errors': self.total_errors,
            'unique_senders': len(self.sender_counts),
            'unique_domains': len(self.domain_counts),
            'final_delay': self.throttler.get_current_delay(),
            'success_rate': (self.total_processed / (self.total_processed + self.total_errors)) * 100 if (self.total_processed + self.total_errors) > 0 else 0,
            'top_domain': max(self.domain_counts.items(), key=lambda x: x[1]) if self.domain_counts else None,
            'top_sender': max(self.sender_counts.items(), key=lambda x: x[1]) if self.sender_counts else None
        }


def main():
    parser = argparse.ArgumentParser(description='Gmail Analyzer - адаптивный анализ отправителей')
    parser.add_argument('--days', type=int, help='Количество дней назад для анализа')
    parser.add_argument('--query', type=str, help='Дополнительный поисковый запрос')
    parser.add_argument('--output', type=str, help='Имя выходного CSV файла')
    parser.add_argument('--json', action='store_true', help='Сохранить также JSON отчет')
    parser.add_argument('--max-emails', type=int, help='Максимальное количество писем для обработки')
    parser.add_argument('--chunk-size', type=int, default=15, help='Размер чанка (по умолчанию 15)')
    parser.add_argument('--initial-delay', type=float, help='Начальная задержка в секундах')
    parser.add_argument('--max-delay', type=float, help='Максимальная задержка в секундах')
    parser.add_argument('--turbo', action='store_true', help='Турбо режим (быстрее, но рискованнее)')
    parser.add_argument('--conservative', action='store_true', help='Консервативный режим (медленнее, но надежнее)')
    
    args = parser.parse_args()

    # Обновляем конфигурацию из аргументов
    if args.chunk_size:
        CONFIG['chunk_size'] = args.chunk_size
    if args.initial_delay:
        CONFIG['initial_delay'] = args.initial_delay
    if args.max_delay:
        CONFIG['max_delay'] = args.max_delay
    
    # Режимы работы
    if args.turbo:
        CONFIG['turbo_mode'] = True
        CONFIG['initial_delay'] = 0.02
        CONFIG['success_threshold'] = 3
        CONFIG['chunk_size'] = 20
        logging.info("🚀 ТУРБО РЕЖИМ активирован!")
    elif args.conservative:
        CONFIG['initial_delay'] = 0.2
        CONFIG['success_threshold'] = 10
        CONFIG['chunk_size'] = 8
        CONFIG['max_delay'] = 15.0
        logging.info("🐌 КОНСЕРВАТИВНЫЙ РЕЖИМ активирован!")

    analyzer = GmailAnalyzer()
    
    try:
        analyzer.count_emails(
            days_back=args.days, 
            query=args.query, 
            max_emails=args.max_emails
        )
        
        # Сохранение результатов
        output_file = args.output or CONFIG['result_file']
        analyzer.save_csv(output_file)
        
        if args.json:
            json_file = output_file.replace('.csv', '_report.json') if args.output else 'report.json'
            analyzer.save_json(json_file)
        
        # Вывод финальной статистики
        stats = analyzer.get_statistics()
        print(f"\n=== ФИНАЛЬНАЯ СТАТИСТИКА ===")
        print(f"Всего обработано: {stats['total_emails']}")
        print(f"Ошибок: {stats['total_errors']}")
        print(f"Процент успеха: {stats['success_rate']:.1f}%")
        print(f"Уникальных отправителей: {stats['unique_senders']}")
        print(f"Уникальных доменов: {stats['unique_domains']}")
        print(f"Финальная задержка: {stats['final_delay']:.2f}s")
        
        if stats['top_sender']:
            print(f"Топ отправитель: {stats['top_sender'][0]} ({stats['top_sender'][1]} писем)")
        if stats['top_domain']:
            print(f"Топ домен: {stats['top_domain'][0]} ({stats['top_domain'][1]} писем)")
            
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())