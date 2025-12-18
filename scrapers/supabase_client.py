#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - VERSÃO OTIMIZADA v3.0
Performance máxima com RPC nativo, batching inteligente e retry logic
"""

import os
import re
import json
import time
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class SupabaseClient:
    """Cliente otimizado para Supabase com connection pooling e retry logic"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        
        # Session com connection pooling e retry
        self.session = self._create_session()
        
        # Cache de disponibilidade da função RPC
        self._rpc_available = None
        self._check_rpc_availability()
    
    def _create_session(self) -> requests.Session:
        """Cria session com connection pooling e retry automático"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET", "PATCH"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def _check_rpc_availability(self) -> bool:
        """Verifica se função RPC está disponível"""
        if self._rpc_available is not None:
            return self._rpc_available
        
        try:
            url = f"{self.url}/rest/v1/rpc/upsert_auctions_v2"
            r = self.session.post(
                url,
                headers=self.headers,
                json={'items': []},
                timeout=5
            )
            self._rpc_available = r.status_code in (200, 201)
            
            if self._rpc_available:
                print("✅ RPC upsert_auctions_v2 disponível (modo otimizado)")
            else:
                print("⚠️ RPC não disponível - execute install.sql")
                
        except Exception as e:
            print(f"⚠️ Erro ao verificar RPC: {e}")
            self._rpc_available = False
        
        return self._rpc_available
    
    def upsert_normalized(self, items: List[Dict]) -> Dict[str, int]:
        """
        UPSERT otimizado com batching inteligente
        Retorna: {'inserted': X, 'updated': Y, 'errors': Z, 'time_ms': T}
        """
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'time_ms': 0}
        
        start_time = time.time()
        
        # Se RPC disponível, usar (mais rápido)
        if self._rpc_available:
            stats = self._upsert_via_rpc(items)
        else:
            print("⚠️ RPC indisponível! Execute install.sql para melhor performance!")
            stats = self._upsert_fallback(items)
        
        stats['time_ms'] = int((time.time() - start_time) * 1000)
        
        return stats
    
    def _upsert_via_rpc(self, items: List[Dict]) -> Dict[str, int]:
        """Método otimizado usando RPC"""
        url = f"{self.url}/rest/v1/rpc/upsert_auctions_v2"
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        total_batches = (len(items) + batch_size - 1) // batch_size
        
        print(f"📦 Processando {len(items)} itens em {total_batches} batches")
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                r = self.session.post(
                    url,
                    headers=self.headers,
                    json={'items': batch},
                    timeout=120
                )
                
                if r.status_code == 200:
                    result = r.json()
                    stats['inserted'] += result.get('inserted', 0)
                    stats['updated'] += result.get('updated', 0)
                    stats['errors'] += result.get('errors', 0)
                    
                    progress = (batch_num / total_batches) * 100
                    print(f"   ✅ [{progress:3.0f}%] Batch {batch_num}/{total_batches}: "
                          f"+{result.get('inserted', 0)} novos, "
                          f"~{result.get('updated', 0)} atualizados")
                else:
                    print(f"   ❌ Batch {batch_num}: HTTP {r.status_code}")
                    stats['errors'] += len(batch)
                    
            except Exception as e:
                print(f"   ❌ Batch {batch_num}: {str(e)[:100]}")
                stats['errors'] += len(batch)
        
        total = stats['inserted'] + stats['updated'] + stats['errors']
        success_rate = ((stats['inserted'] + stats['updated']) / total * 100) if total > 0 else 0
        print(f"\n📊 RESULTADO: {stats['inserted']} novos | "
              f"{stats['updated']} atualizados | "
              f"{stats['errors']} erros | "
              f"{success_rate:.1f}% sucesso")
        
        return stats
    
    def _upsert_fallback(self, items: List[Dict]) -> Dict[str, int]:
        """Fallback sem RPC"""
        url = f"{self.url}/rest/v1/auctions"
        
        upsert_headers = self.headers.copy()
        upsert_headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 200
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            
            try:
                r = self.session.post(
                    url,
                    headers=upsert_headers,
                    json=batch,
                    timeout=30
                )
                
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"   ✅ Batch {i//batch_size + 1}: {len(batch)} processados")
                else:
                    print(f"   ⚠️ Batch {i//batch_size + 1}: Status {r.status_code}")
                    stats['errors'] += len(batch)
                    
            except Exception as e:
                print(f"   ❌ Erro: {str(e)[:100]}")
                stats['errors'] += len(batch)
        
        return stats
    
    def insert_normalized(self, items: List[Dict]) -> int:
        """Método legado - usa upsert_normalized()"""
        result = self.upsert_normalized(items)
        return result['inserted'] + result['updated']
    
    def __del__(self):
        """Fecha session ao destruir objeto"""
        if hasattr(self, 'session'):
            self.session.close()


# ============================================================
# HELPERS OTIMIZADOS
# ============================================================

_REGEX_CACHE = {
    'whitespace': re.compile(r'\s+'),
    'control_chars': re.compile(r'[\x00-\x1f\x7f-\x9f]'),
    'state_end': re.compile(r'[-/]\s*([A-Z]{2})\s*$'),
    'date_iso': re.compile(r'(\d{4}-\d{2}-\d{2})'),
    'date_br': re.compile(r'(\d{2})/(\d{2})/(\d{4})'),
    'clean_id': re.compile(r'[^a-zA-Z0-9-]+')
}

_UFS = {'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
        'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'}


def clean_text(text: Optional[str], max_len: Optional[int] = None) -> Optional[str]:
    if not text:
        return None
    text = _REGEX_CACHE['whitespace'].sub(' ', str(text)).strip()
    text = _REGEX_CACHE['control_chars'].sub('', text)
    if max_len and len(text) > max_len:
        text = text[:max_len].rsplit(' ', 1)[0] + '...'
    return text if text else None


def extract_state(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    
    text_upper = text.upper()
    match = _REGEX_CACHE['state_end'].search(text_upper)
    if match:
        uf = match.group(1)
        return uf if uf in _UFS else None
    
    for word in text_upper.split():
        if word in _UFS:
            return word
    
    return None


def parse_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        clean = re.sub(r'[^\d,]', '', value).replace(',', '.')
        try:
            return float(clean) if clean else None
        except:
            return None
    return None


def parse_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    
    match = _REGEX_CACHE['date_iso'].search(date_str)
    if match:
        parsed = match.group(1)
    else:
        match = _REGEX_CACHE['date_br'].search(date_str)
        if match:
            day, month, year = match.groups()
            parsed = f"{year}-{month}-{day}"
        else:
            return None
    
    try:
        year = int(parsed[:4])
        current_year = datetime.now().year
        if year < (current_year - 2) or year > (current_year + 5):
            return None
        return parsed
    except:
        return None


def generate_clean_external_id(source: str, raw_id: Any) -> str:
    if not raw_id:
        return f"{source}_unknown_{int(time.time() * 1000)}"
    clean = _REGEX_CACHE['clean_id'].sub('_', str(raw_id).lower())
    clean = clean.strip('_')[:100]
    return f"{source}_{clean}"


# ============================================================
# NORMALIZER SUPERBID
# ============================================================

def normalize_superbid(data: List[Dict]) -> List[Dict]:
    """Normaliza Superbid - versão otimizada"""
    results = []
    filtered = 0
    
    for item in data:
        external_id = item.get('external_id')
        link = item.get('link')
        store_name = item.get('store_name')
        
        if not external_id or not link:
            continue
        
        if not store_name:
            filtered += 1
            continue
        
        # Parse de data
        auction_date = item.get('auction_date')
        if auction_date and isinstance(auction_date, str):
            try:
                auction_date = auction_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(auction_date)
                auction_date = dt.strftime('%Y-%m-%d %H:%M:%S%z')
            except:
                auction_date = None
        
        # Estado
        state = item.get('state') or extract_state(item.get('address', ''))
        
        results.append({
            'source': 'superbid',
            'external_id': external_id,
            'link': link,
            'title': clean_text(item.get('title'), 200),
            'value': item.get('value'),
            'address': item.get('address'),
            'auction_date': auction_date,
            'auction_name': item.get('auction_name'),
            'auction_type': item.get('auction_type'),
            'category': item.get('category', 'outros'),
            'city': item.get('city'),
            'days_remaining': item.get('days_remaining'),
            'description': item.get('description'),
            'description_preview': item.get('description_preview'),
            'lot_number': item.get('lot_number'),
            'metadata': item.get('metadata', {}),
            'state': state,
            'store_name': store_name,
            'total_bidders': item.get('total_bidders', 0),
            'total_bids': item.get('total_bids', 0),
            'total_visits': item.get('total_visits', 0),
            'value_text': item.get('value_text'),
        })
    
    if filtered > 0:
        print(f"🚫 Filtrados {filtered} itens inválidos")
    
    return results


def normalize(source: str, data: Any) -> List[Dict]:
    """Normaliza dados de qualquer fonte"""
    if source.lower() != 'superbid':
        raise ValueError(f"Fonte não suportada: {source}")
    
    normalized = normalize_superbid(data)
    
    # Validação final
    return [
        item for item in normalized
        if item.get('external_id') 
        and item.get('link')
        and (not item.get('state') or len(item['state']) == 2)
    ]
