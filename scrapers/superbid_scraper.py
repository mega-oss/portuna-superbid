#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERBID SCRAPER - VERSÃO CORRIGIDA v2
Correção: Tratamento de None nos campos de texto
"""

import json
import time
import requests
import os
import sys
import random
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

BASE_URL = "https://exchange.superbid.net"
API_BASE = "https://offer-query.superbid.net"
OUTPUT_DIR = Path("superbid_data")
OUTPUT_DIR.mkdir(exist_ok=True)

MAX_EXECUTION_TIME = 10680
SAVE_CHECKPOINT_EVERY = 1000
REQUEST_TIMEOUT = 45
REQUEST_DELAY_MIN = 2
REQUEST_DELAY_MAX = 5
CATEGORY_DELAY_MIN = 10
CATEGORY_DELAY_MAX = 20

CATEGORIES = {
    "carros-motos": "Carros & Motos",
    "caminhoes-onibus": "Caminhões & Ônibus",
    "imoveis": "Imóveis",
    "maquinas-pesadas-agricolas": "Máquinas Pesadas & Agrícolas",
    "tecnologia": "Tecnologia",
    "eletrodomesticos": "Eletrodomésticos",
    "moveis-e-decoracao": "Móveis e Decoração",
    "industrial-maquinas-equipamentos": "Industrial, Máquinas & Equipamentos",
    "materiais-para-construcao-civil": "Materiais para Construção Civil",
    "movimentacao-transporte": "Movimentação & Transporte",
    "embarcacoes-aeronaves": "Embarcações & Aeronaves",
    "partes-e-pecas": "Partes e Peças",
    "sucatas-materiais-residuos": "Sucatas, Materiais & Resíduos",
    "bolsas-canetas-joias-e-relogios": "Bolsas, Canetas, Joias e Relógios",
    "artes-decoracao-colecionismo": "Artes, Decoração & Colecionismo",
    "oportunidades": "Oportunidades",
    "cozinhas-e-restaurantes": "Cozinhas e Restaurantes",
    "alimentos-e-bebidas": "Alimentos e Bebidas",
    "animais": "Animais",
}


class SuperbidScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9",
            "origin": BASE_URL,
            "referer": f"{BASE_URL}/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        self.start_time = time.time()
        self.should_stop = False
        self.max_retries = 3
        self.filtered_stats = {
            'demo_seller': 0,
            'demo_auctioneer': 0,
            'deploy_text': 0,
            'no_store': 0
        }
    
    def check_timeout(self) -> bool:
        elapsed = time.time() - self.start_time
        return elapsed > MAX_EXECUTION_TIME or self.should_stop
    
    def random_delay(self, min_sec: float, max_sec: float, reason: str = ""):
        delay = random.uniform(min_sec, max_sec)
        if reason:
            print(f"   ⏳ {reason} ({delay:.1f}s)...")
        time.sleep(delay)
    
    def is_test_offer(self, offer: dict) -> Tuple[bool, str]:
        """
        Verifica se a oferta é de teste/demo
        Retorna: (is_test, reason)
        """
        seller = offer.get("seller", {})
        auction = offer.get("auction", {})
        product = offer.get("product", {})
        store = offer.get("store", {})
        
        # 1. Verifica store_name NULL
        store_name = store.get("name")
        if not store_name:
            return True, "no_store"
        
        # 2. Verifica "Vendedor Demo" - COM PROTEÇÃO CONTRA None
        seller_name = seller.get("name") or ""
        seller_name = seller_name.lower() if seller_name else ""
        if "vendedor demo" in seller_name or "demo" in seller_name:
            return True, "demo_seller"
        
        # 3. Verifica "Corretor Demo" ou "Leiloeiro Demo" - COM PROTEÇÃO CONTRA None
        auctioneer = auction.get("auctioneer") or ""
        auctioneer = auctioneer.lower() if auctioneer else ""
        if "demo" in auctioneer or "corretor demo" in auctioneer or "leiloeiro demo" in auctioneer:
            return True, "demo_auctioneer"
        
        # 4. Verifica "deploy" no título ou descrição - COM PROTEÇÃO CONTRA None
        title = product.get("shortDesc") or ""
        title = title.lower() if title else ""
        
        description = offer.get("offerDescription", {}).get("offerDescription") or ""
        description = description.lower() if description else ""
        
        if "deploy" in title or "deploy" in description:
            return True, "deploy_text"
        
        return False, ""
    
    def fetch_category_offers(self, category_slug: str, max_pages: int = None) -> List[dict]:
        print(f"\n📦 {CATEGORIES.get(category_slug, category_slug)}")
        
        offers = []
        page = 1
        checkpoint_counter = 0
        consecutive_errors = 0
        local_filtered = {'demo_seller': 0, 'demo_auctioneer': 0, 'deploy_text': 0, 'no_store': 0}
        
        while not self.check_timeout():
            if max_pages and page > max_pages:
                print(f"   ✅ Limite de {max_pages} páginas atingido")
                break
            
            url = f"{API_BASE}/seo/offers/"
            params = {
                "urlSeo": f"{BASE_URL}/categorias/{category_slug}",
                "locale": "pt_BR",
                "orderBy": "offerDetail.percentDiffReservedPriceOverFipePrice:asc",
                "pageNumber": page,
                "pageSize": 100,
                "portalId": "[2,15]",
                "preOrderBy": "orderByFirstOpenedOffersAndSecondHasPhoto",
                "requestOrigin": "marketplace",
                "searchType": "openedAll",
                "timeZoneId": "America/Sao_Paulo",
            }
            
            try:
                r = self.session.get(url, params=params, headers=self.headers, timeout=REQUEST_TIMEOUT)
                
                if r.status_code == 404:
                    print(f"   ✅ Fim: página {page} retornou 404")
                    break
                
                if r.status_code == 200:
                    try:
                        data = r.json()
                    except json.JSONDecodeError:
                        print(f"   ⚠️ Erro JSON na página {page}")
                        consecutive_errors += 1
                        if consecutive_errors >= self.max_retries:
                            break
                        continue
                    
                    page_offers = data.get("offers", [])
                    
                    if not page_offers or len(page_offers) == 0:
                        print(f"   ✅ Fim: página {page} vazia")
                        break
                    
                    # Filtra ofertas de teste e ativas
                    active = []
                    for offer in page_offers:
                        # Verifica se é teste/demo
                        is_test, reason = self.is_test_offer(offer)
                        if is_test:
                            local_filtered[reason] += 1
                            self.filtered_stats[reason] += 1
                            continue
                        
                        # Verifica se está ativa
                        end_date = offer.get("endDate")
                        if end_date:
                            try:
                                end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                                if end_dt > datetime.now(end_dt.tzinfo):
                                    active.append(offer)
                            except:
                                active.append(offer)
                        else:
                            active.append(offer)
                    
                    offers.extend(active)
                    print(f"   Pág {page}: +{len(active)} válidos | Total: {len(offers)}")
                    
                    if len(page_offers) < 10:
                        print(f"   ✅ Fim: Última página com {len(page_offers)} ofertas")
                        break
                    
                    if len(offers) >= (checkpoint_counter + 1) * SAVE_CHECKPOINT_EVERY:
                        checkpoint_counter += 1
                        self.save_checkpoint(offers, category_slug, checkpoint_counter)
                    
                    page += 1
                    consecutive_errors = 0
                    self.random_delay(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, "Próxima página")
                    
                elif r.status_code == 429:
                    wait_time = random.randint(15, 30)
                    print(f"   ⚠️ Rate limit, aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    consecutive_errors += 1
                    if consecutive_errors >= self.max_retries:
                        break
                else:
                    print(f"   ⚠️ Status {r.status_code} na página {page}")
                    consecutive_errors += 1
                    if consecutive_errors >= self.max_retries:
                        break
                    
            except requests.exceptions.Timeout:
                consecutive_errors += 1
                wait_time = random.randint(10, 20)
                print(f"   ⚠️ Timeout na página {page} ({consecutive_errors}/{self.max_retries})")
                if consecutive_errors >= self.max_retries:
                    break
                time.sleep(wait_time)
            except Exception as e:
                consecutive_errors += 1
                print(f"   ❌ Erro na página {page}: {e}")
                if consecutive_errors >= self.max_retries:
                    break
                time.sleep(random.randint(10, 20))
        
        if self.check_timeout():
            print(f"\n⏰ Timeout global na página {page}")
        
        # Mostra estatísticas de filtros da categoria
        total_filtered = sum(local_filtered.values())
        if total_filtered > 0:
            print(f"   🚫 Filtrados nesta categoria: {total_filtered}")
            if local_filtered['no_store'] > 0:
                print(f"      • Sem loja: {local_filtered['no_store']}")
            if local_filtered['demo_seller'] > 0:
                print(f"      • Vendedor Demo: {local_filtered['demo_seller']}")
            if local_filtered['demo_auctioneer'] > 0:
                print(f"      • Leiloeiro Demo: {local_filtered['demo_auctioneer']}")
            if local_filtered['deploy_text'] > 0:
                print(f"      • Texto 'deploy': {local_filtered['deploy_text']}")
        
        print(f"   📊 Total coletado: {len(offers)} ofertas válidas")
        return offers
    
    def save_checkpoint(self, offers: List[dict], category_slug: str, checkpoint_num: int):
        normalized = [self.normalize_to_schema(o, category_slug) for o in offers]
        unique = {o["external_id"]: o for o in normalized}
        normalized = list(unique.values())
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"superbid_{category_slug}_ckpt{checkpoint_num}_{timestamp}.json"
        self.save_json(normalized, filename)
        
        print(f"   💾 Checkpoint {checkpoint_num}: enviando {len(normalized)} itens...")
        upload_to_supabase(normalized)
    
    def extract_city_state(self, city_text: str) -> Tuple[Optional[str], Optional[str]]:
        if not city_text:
            return None, None
        
        city_text = city_text.strip()
        state = None
        city = city_text
        
        if '/' in city_text:
            parts = city_text.split('/')
            city = parts[0].strip()
            state = parts[1].strip() if len(parts) > 1 else None
        elif ' - ' in city_text:
            parts = city_text.split(' - ')
            city = parts[0].strip()
            state = parts[1].strip() if len(parts) > 1 else None
        
        if state and (len(state) != 2 or not state.isupper()):
            state = None
        
        return city, state
    
    def normalize_to_schema(self, offer: dict, category_slug: str) -> Dict:
        product = offer.get("product", {})
        auction = offer.get("auction", {})
        detail = offer.get("offerDetail", {})
        seller = offer.get("seller", {})
        store = offer.get("store", {})
        
        offer_id = str(offer.get("id"))
        external_id = f"superbid_{offer_id}"
        title = product.get("shortDesc", "Sem título").strip()
        category = CATEGORIES.get(category_slug, "Outros")
        
        value = detail.get("currentMinBid") or detail.get("initialBidValue")
        value_text = detail.get("currentMinBidFormatted") or detail.get("initialBidValueFormatted")
        
        seller_city_text = seller.get("city", "")
        city, state = self.extract_city_state(seller_city_text)
        
        full_description = offer.get("offerDescription", {}).get("offerDescription", "")
        description_preview = full_description[:150] if full_description else title[:150]
        
        end_date_str = offer.get("endDate")
        auction_date = None
        days_remaining = None
        
        if end_date_str:
            try:
                auction_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                days_remaining = max(0, (auction_date - datetime.now(auction_date.tzinfo)).days)
            except:
                pass
        
        auction_type = auction.get("modalityDesc", "Leilão")
        auction_name = auction.get("desc")
        store_name = store.get("name")
        lot_number = offer.get("lotNumber")
        
        total_visits = offer.get("visits", 0)
        total_bids = offer.get("totalBids", 0)
        total_bidders = offer.get("totalBidders", 0)
        
        address = seller_city_text
        link = f"{BASE_URL}/oferta/{offer_id}"
        
        gallery = product.get("galleryJson", [])
        total_fotos = len([i for i in gallery if i.get("link")]) if gallery else 0
        
        metadata = {
            "leiloeiro": auction.get("auctioneer"),
            "quantidade_lote": offer.get("quantityInLot"),
            "vendedor": {
                "nome": seller.get("name"),
                "empresa": seller.get("company"),
            },
            "preco_detalhado": {
                "inicial": detail.get("initialBidValue"),
                "inicial_fmt": detail.get("initialBidValueFormatted"),
                "lance_minimo": detail.get("currentMinBid"),
                "lance_minimo_fmt": detail.get("currentMinBidFormatted"),
                "lance_maximo": detail.get("currentMaxBid"),
                "lance_maximo_fmt": detail.get("currentMaxBidFormatted"),
            },
            "midia": {
                "total_fotos": total_fotos,
                "total_videos": product.get("videoUrlCount", 0),
            },
            "datas": {
                "criacao": offer.get("createAt"),
                "publicacao": offer.get("publishedAt"),
            }
        }
        
        return {
            "source": "superbid",
            "external_id": external_id,
            "title": title,
            "category": category,
            "value": value,
            "value_text": value_text,
            "city": city,
            "state": state,
            "description_preview": description_preview,
            "auction_date": auction_date.isoformat() if auction_date else None,
            "days_remaining": days_remaining,
            "auction_type": auction_type,
            "auction_name": auction_name,
            "store_name": store_name,
            "lot_number": lot_number,
            "total_visits": total_visits,
            "total_bids": total_bids,
            "total_bidders": total_bidders,
            "description": full_description,
            "address": address,
            "link": link,
            "metadata": metadata,
        }
    
    def scrape_all(self, max_pages: int = None) -> List[Dict]:
        print("\n" + "="*60)
        print("🚀 SUPERBID - SCRAPING COMPLETO")
        print("="*60)
        
        all_offers = []
        category_count = 0
        
        for slug, name in CATEGORIES.items():
            if self.check_timeout():
                print("\n⏰ Timeout global")
                break
            
            category_count += 1
            print(f"\n📋 Categoria {category_count}/{len(CATEGORIES)}: {name}")
            
            offers = self.fetch_category_offers(slug, max_pages=max_pages)
            
            if offers:
                normalized = [self.normalize_to_schema(o, slug) for o in offers]
                
                # Remove duplicatas dentro da categoria
                unique = {o["external_id"]: o for o in normalized}
                normalized = list(unique.values())
                
                print(f"   ✅ {len(normalized)} ofertas normalizadas")
                
                # NOVO: Salva e faz upload ao final de cada categoria
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"superbid_{slug}_final_{timestamp}.json"
                self.save_json(normalized, filename)
                
                print(f"   📤 Enviando categoria para Supabase...")
                upload_to_supabase(normalized)
                
                all_offers.extend(normalized)
            else:
                print(f"   ⚠️ Nenhuma oferta encontrada")
            
            if category_count < len(CATEGORIES) and not self.check_timeout():
                self.random_delay(CATEGORY_DELAY_MIN, CATEGORY_DELAY_MAX, 
                                f"Próxima categoria ({category_count + 1}/{len(CATEGORIES)})")
        
        # Remove duplicatas globais
        unique = {o["external_id"]: o for o in all_offers}
        all_offers = list(unique.values())
        
        # Mostra estatísticas globais de filtros
        total_filtered = sum(self.filtered_stats.values())
        if total_filtered > 0:
            print(f"\n🚫 TOTAL FILTRADO: {total_filtered} ofertas de teste/demo")
            print(f"   • Sem loja (store_name NULL): {self.filtered_stats['no_store']}")
            print(f"   • Vendedor Demo: {self.filtered_stats['demo_seller']}")
            print(f"   • Leiloeiro Demo: {self.filtered_stats['demo_auctioneer']}")
            print(f"   • Texto 'deploy': {self.filtered_stats['deploy_text']}")
        
        print(f"\n✅ Total único global: {len(all_offers)} ofertas válidas")
        return all_offers
    
    def save_json(self, data, filename: str):
        filepath = OUTPUT_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        print(f"   💾 Salvo: {filepath}")
        return filepath


def upload_to_supabase(offers: List[Dict]):
    """Upload com tratamento de erro melhorado"""
    try:
        from supabase_client import SupabaseClient
        
        client = SupabaseClient()
        
        print(f"   📤 Enviando {len(offers)} itens para Supabase...")
        
        # Salva dados RAW
        client.insert_raw('superbid', offers)
        
        # Insere normalizados
        inserted = client.insert_normalized(offers)
        
        if inserted > 0:
            print(f"   ✅ {inserted} itens processados com sucesso")
            return True
        else:
            print("   ℹ️ Nenhum item novo (todos já existem)")
            return True
        
    except ImportError:
        print("\n   ❌ supabase_client.py não encontrado")
        print("   ⚠️ Os dados foram salvos localmente mas não enviados ao Supabase")
        return False
    except Exception as e:
        print(f"\n   ❌ Erro ao enviar para Supabase: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Superbid Scraper')
    parser.add_argument('--categoria', type=str, help='Slug da categoria')
    parser.add_argument('--full-update', action='store_true', help='Todas as categorias')
    parser.add_argument('--max-pages', type=int, help='Limite de páginas')
    
    args = parser.parse_args()
    
    print("="*60)
    print("🚀 SUPERBID SCRAPER")
    print("="*60)
    print(f"⏰ Início: {datetime.now().strftime('%H:%M:%S')}")
    print(f"⏱️ Timeout: 2h58min\n")
    
    scraper = SuperbidScraper()
    
    try:
        if args.full_update:
            print(f"📦 Processando {len(CATEGORIES)} categorias...\n")
            offers = scraper.scrape_all(max_pages=args.max_pages)
            
            if offers:
                # Salva consolidado final
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                scraper.save_json(offers, f"superbid_full_consolidated_{timestamp}.json")
                print("\n✅ Arquivo consolidado salvo")
            else:
                print("\n⚠️ Nenhuma oferta coletada")
        
        elif args.categoria:
            if args.categoria not in CATEGORIES:
                print(f"❌ Categoria '{args.categoria}' não existe!")
                print(f"\n📋 Categorias disponíveis:")
                for key in sorted(CATEGORIES.keys()):
                    print(f"   • {key}")
                sys.exit(1)
            
            offers = scraper.fetch_category_offers(args.categoria, max_pages=args.max_pages)
            
            if offers:
                normalized = [scraper.normalize_to_schema(o, args.categoria) for o in offers]
                unique = {o["external_id"]: o for o in normalized}
                normalized = list(unique.values())
                
                print(f"\n✅ Total único: {len(normalized)} ofertas")
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                scraper.save_json(normalized, f"superbid_{args.categoria}_final_{timestamp}.json")
                upload_to_supabase(normalized)
            else:
                print("\n⚠️ Nenhuma oferta encontrada")
        
        else:
            print("❌ Use --categoria <slug> ou --full-update")
            print(f"\n📋 Categorias disponíveis:")
            for key in sorted(CATEGORIES.keys()):
                print(f"   • {key}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrompido pelo usuário")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        elapsed = time.time() - scraper.start_time
        elapsed_min = int(elapsed // 60)
        elapsed_sec = int(elapsed % 60)
        
        print(f"\n⏰ Fim: {datetime.now().strftime('%H:%M:%S')}")
        print(f"⏱️ Tempo total: {elapsed_min}min {elapsed_sec}s")
        print("="*60)
    
    sys.exit(0)


if __name__ == "__main__":
    main()