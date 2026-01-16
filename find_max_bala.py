#!/usr/bin/env python3
"""
Script to find dates with maximum total Shad Bala
"""
import sys
sys.path.insert(0, '.')
from kundali_maker import kundali, BirthInput
from datetime import datetime, timedelta
import random

def calculate_total_shad_bala(year: int, month: int, day: int, hour: int = 12) -> dict:
    """Calculate total Shad Bala for a given date."""
    try:
        birth_input = BirthInput(
            year=year, month=month, day=day,
            hour=hour, minute=0, second=0,
            tz_offset_hours=0,  # UTC
            latitude=28.6139,  # Delhi
            longitude=77.2090,
            ayanamsha=1,  # Lahiri
            ephe_path="./ephe"
        )
        result = kundali(birth_input)
        
        total = 0
        planet_totals = {}
        for planet, bala in result['shad_bala'].items():
            total += bala['total_bala']
            planet_totals[planet] = bala['total_bala']
        
        strong_count = sum(1 for p, b in result['shad_bala'].items() if b['strength'] == 'Strong')
        
        return {
            'date': f"{year}-{month:02d}-{day:02d}",
            'total': total,
            'strong_count': strong_count,
            'planets': planet_totals,
            'lagna': result['lagna']['sign']
        }
    except Exception as e:
        return None

def search_max_bala():
    """Search for dates with maximum Shad Bala."""
    print("Searching for dates with maximum total Shad Bala...")
    print("=" * 60)
    
    best_results = []
    
    # Search through various historical dates
    # Focus on dates where planets might be in strong positions
    years_to_check = list(range(1900, 2100, 5))  # Every 5 years
    
    for year in years_to_check:
        for month in [1, 4, 7, 10]:  # Quarterly check
            for day in [1, 15]:
                result = calculate_total_shad_bala(year, month, day)
                if result:
                    best_results.append(result)
    
    # Sort by total Shad Bala
    best_results.sort(key=lambda x: x['total'], reverse=True)
    
    print("\nTop 10 Dates with Highest Total Shad Bala:")
    print("-" * 60)
    
    for i, result in enumerate(best_results[:10], 1):
        print(f"\n{i}. {result['date']} (Lagna: {result['lagna']})")
        print(f"   Total Shad Bala: {result['total']}")
        print(f"   Strong Planets: {result['strong_count']}/8")
        print(f"   Planets: {result['planets']}")
    
    # Find theoretical maximum
    print("\n" + "=" * 60)
    print("THEORETICAL MAXIMUM SHAD BALA:")
    print("-" * 60)
    print("""
For maximum Shad Bala, all planets would need:
- Exalted position (+60 Sthana Bala)
- Moolatrikona position (+45 additional)
- In their Dig Bala house (+60)
- Retrograde for outer planets (+60 Chesta Bala)
- Angular house position (+60)
- Max Kala Bala (+30)
- Natural strength (varies by planet)
- Drik Bala (+30)

Theoretical max per planet: ~350-400 points
Theoretical total: ~2800-3200 points

However, this is IMPOSSIBLE because:
1. Exaltation signs and Dig Bala houses don't overlap for most planets
2. Inner planets (Sun, Mercury, Venus) don't go retrograde
3. Planetary positions are constrained by actual orbital mechanics
""")
    
    return best_results[0] if best_results else None

if __name__ == "__main__":
    best = search_max_bala()
    
    if best:
        print("\n" + "=" * 60)
        print(f"BEST DATE FOUND: {best['date']}")
        print(f"Total Shad Bala: {best['total']}")
        print("=" * 60)
