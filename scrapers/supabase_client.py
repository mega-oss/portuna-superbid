#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - SUPERBID
"""

import os
import re
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta


class SupabaseClient:
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
    
    def insert_raw(self, source: str, data: Any) -> bool:
        url = f"{self.url}/rest/v1/raw_auctions"
        payload = {'source': source, 'data': data}
        
        try:
            r = requests.post(url, headers=self.headers, json=payload, timeout=30)
            r.raise_for_status()
            print(f"✅ RAW salvo")
            return True
        except Exception as e:
            print(f"❌ Erro RAW: {e}")
            return False
    
    def insert_normalized(self, items: List[Dict]) -> int:
        if not items:
            return 0
        
        url = f"{self.url}/rest/v1/auctions"
        total_inserted = 0
        total_duplicated = 0
        
        for i in range(0, len(items), 500):
            batch = items[i:i+500]
            try:
                r = requests.post(url, headers=self.headers, json=batch, timeout=60)
                
                # 🔍 DEBUG DETALHADO
                if r.status_code == 400:
                    print(f"\n❌ ERRO 400 - Batch {i//500 + 1}")
                    print(f"Resposta do servidor: {r.text[:1000]}")
                    print(f"\n🔍 Primeiro item do batch:")
                    import json
                    print(json.dumps(batch[0], indent=2, ensure_ascii=False, default=str))
                    continue
                
                if r.status_code == 409:
                    print(f"   ⚪ Batch {i//500 + 1}: {len(batch)} duplicados")
                    total_duplicated += len(batch)
                elif r.status_code in (200, 201):
                    print(f"   ✅ Batch {i//500 + 1}: {len(batch)} novos")
                    total_inserted += len(batch)
                else:
                    r.raise_for_status()
                    
            except Exception as e:
                if "409" in str(e) or "duplicate key" in str(e).lower():
                    print(f"   ⚪ Batch {i//500 + 1}: {len(batch)} duplicados")
                    total_duplicated += len(batch)
                else:
                    print(f"   ❌ Erro batch {i//500 + 1}: {e}")
        
        print(f"\n📊 Inseridos: {total_inserted} | Duplicados: {total_duplicated}")
        return total_inserted


# ============================================================
# HELPERS
# ============================================================

def clean_text(text: Optional[str], max_len: Optional[int] = None) -> Optional[str]:
    if not text:
        return None
    text = re.sub(r'\s+', ' ', str(text)).strip()
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    if max_len and len(text) > max_len:
        text = text[:max_len].rsplit(' ', 1)[0] + '...'
    return text if text else None


def extract_state(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    match = re.search(r'[-/]\s*([A-Z]{2})\s*$', text.upper())
    if match:
        return match.group(1)
    ufs = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
    for uf in ufs:
        if re.search(rf'\b{uf}\b', text.upper()):
            return uf
    return None


def parse_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = re.sub(r'[^\d,]', '', value).replace(',', '.')
        try:
            return float(value) if value else None
        except:
            return None
    return None


def parse_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    
    parsed_date = None
    
    # ISO 8601 completo
    match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
    if match:
        parsed_date = match.group(1)
    
    # DD/MM/YYYY
    if not parsed_date:
        match = re.search(r'(\d{2})/(\d{2})/(\d{4})', date_str)
        if match:
            day, month, year = match.groups()
            parsed_date = f"{year}-{month}-{day}"
    
    # Valida
    if parsed_date:
        try:
            date_obj = datetime.strptime(parsed_date, '%Y-%m-%d')
            hoje = datetime.now()
            dois_anos_atras = hoje - timedelta(days=730)
            if date_obj < dois_anos_atras:
                return None
            return parsed_date
        except:
            return None
    
    return None


def generate_clean_external_id(source: str, raw_id: Any) -> str:
    if not raw_id:
        return f"{source}_unknown_{int(datetime.now().timestamp())}"
    clean_id = re.sub(r'[^a-zA-Z0-9-]', '_', str(raw_id).lower())
    clean_id = re.sub(r'_+', '_', clean_id).strip('_')
    return f"{source}_{clean_id}"


# ============================================================
# NORMALIZER
# ============================================================

def normalize_superbid(data: List[Dict]) -> List[Dict]:
    """Normaliza Superbid - usa campos do schema do banco"""
    results = []
    filtered_count = 0
    
    for item in data:
        external_id = item.get('external_id')
        link = item.get('link')
        store_name = item.get('store_name')
        
        # 🚫 FILTRO: Ignora itens sem external_id, link ou store_name
        if not external_id or not link:
            continue
        
        if not store_name:
            filtered_count += 1
            continue
        
        # Extrai cidade/estado do address
        address = item.get('address', '')
        city = item.get('city')
        state = item.get('state')
        
        if not state and address:
            state = extract_state(address)
        
        # Converte auction_date de ISO string para timestamp
        auction_date = item.get('auction_date')
        if auction_date and isinstance(auction_date, str):
            try:
                # Remove timezone info se existir
                auction_date = auction_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(auction_date)
                auction_date = dt.strftime('%Y-%m-%d %H:%M:%S%z')
            except:
                auction_date = None
        
        results.append({
            'source': 'superbid',
            'external_id': external_id,
            'category': item.get('category', 'outros'),
            'title': clean_text(item.get('title'), 200),
            'description': item.get('description'),
            'description_preview': item.get('description_preview'),
            'value': item.get('value'),
            'value_text': item.get('value_text'),
            'city': city,
            'state': state,
            'address': address,
            'auction_date': auction_date,
            'days_remaining': item.get('days_remaining'),
            'link': link,
            'metadata': item.get('metadata', {}),
            # Campos específicos Superbid
            'auction_type': item.get('auction_type'),
            'auction_name': item.get('auction_name'),
            'store_name': store_name,
            'lot_number': item.get('lot_number'),
            'total_visits': item.get('total_visits', 0),
            'total_bids': item.get('total_bids', 0),
            'total_bidders': item.get('total_bidders', 0),
        })
    
    if filtered_count > 0:
        print(f"🚫 Filtrados {filtered_count} itens com store_name NULL (deploys teste)")
    
    return results


def normalize(source: str, data: Any) -> List[Dict]:
    if source.lower() != 'superbid':
        raise ValueError(f"Fonte não suportada: {source}")
    
    normalized = normalize_superbid(data)
    
    # Validação final
    valid_items = []
    for item in normalized:
        if not item.get('external_id') or not item.get('link'):
            continue
        if item.get('state') and len(item['state']) != 2:
            item['state'] = None
        valid_items.append(item)
    
    return valid_items