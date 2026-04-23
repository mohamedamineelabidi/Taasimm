"""HCP RGPH 2024 province-level indicator loader.

The 6 CSVs under data/hcp-data-casa/ are aggregated at the Préfecture de
Casablanca level. They do NOT contain per-arrondissement breakdowns.
We expose their values as calibration priors applied uniformly to all 16
arrondissements in downstream analytics.

Per-arrondissement population lives separately in
data/casa_arrondissement_population_2024.csv (transcribed from the HCP
interactive portal screenshot).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "hcp-data-casa"
POP_CSV = Path(__file__).resolve().parent.parent / "data" / "casa_arrondissement_population_2024.csv"


def _read_indicator_file(path: Path) -> pd.DataFrame:
    """Read one HCP CSV; normalise column name variants (Valeur/Valeurs)."""
    df = pd.read_csv(path)
    value_col = next(c for c in df.columns if c.startswith("Valeur"))
    df = df.rename(columns={value_col: "value"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _pick(df: pd.DataFrame, indicator: str, milieu: str = "Ensemble", sexe: str | None = "Ensemble") -> float | None:
    """Return the scalar value for (indicator, milieu, sexe) or None if absent."""
    mask = (df["Titre de l'indicateur"].str.strip() == indicator.strip()) & (df["Milieu"] == milieu)
    if "Sexe" in df.columns and sexe is not None:
        mask &= df["Sexe"] == sexe
    hit = df.loc[mask, "value"].dropna()
    if hit.empty:
        return None
    return float(hit.iloc[0])


def load_province_priors() -> Dict[str, float]:
    """Load HCP 2024 province-level priors as a flat dict.

    Returns a dict of calibration constants used as uniform priors over
    all 16 arrondissements (HCP does not publish arrondissement breakdowns).
    """
    frames = {p.stem: _read_indicator_file(p) for p in sorted(DATA_DIR.glob("*.csv"))}

    # Merge the ones that have Sexe into one; the ones without Sexe kept separately.
    demog = pd.concat([f for f in frames.values() if "Sexe" in f.columns], ignore_index=True)
    hh = pd.concat([f for f in frames.values() if "Sexe" not in f.columns], ignore_index=True)

    priors: Dict[str, float] = {
        "population_province": _pick(demog, "Population municipale"),
        "population_female": _pick(demog, "Population municipale", sexe="Féminin"),
        "population_male": _pick(demog, "Population municipale", sexe="Masculin"),
        "share_under_15_pct": _pick(demog, "Part de la population de moins de 15 ans (%)"),
        "share_15_59_pct": _pick(demog, "Part de la population de 15-59 ans (%)"),
        "share_60_plus_pct": _pick(demog, "Part de la population de 60 ans et plus (%)"),
        "fertility_icf": _pick(demog, "Indicateur conjoncturel de fécondité"),
        "households_total": _pick(hh, "Nombre de ménages", sexe=None),
        "household_size_mean": _pick(hh, "Taille moyenne des ménages", sexe=None),
        "share_apartment_pct": _pick(hh, "Part des ménages sédentaires vivant dans un appartement (%)", sexe=None),
        "share_bidonville_pct": _pick(
            hh,
            "Part des ménages sédentaires vivant dans une maison sommaire ou dans un bidonville (%)",
            sexe=None,
        ),
        "share_owner_pct": _pick(hh, "Part des ménages sédentaires propriétaires (%)", sexe=None),
        "share_renter_pct": _pick(hh, "Part des ménages sédentaires locataires (%)", sexe=None),
        "electricity_access_pct": _pick(hh, "Part des ménages sédentaires disposant de l'électricité (%)", sexe=None),
        "water_access_pct": _pick(hh, "Part des ménages sédentaires disposant de l'eau courante (%)", sexe=None),
        # Employment / education (if present in remaining files)
        "salaried_share_pct": _pick(demog, "Part des salariés parmi les actifs occupés de 15 ans et plus (%)"),
        "school_enrolment_6_11_pct": _pick(demog, "Taux de scolarisation des 6-11 ans en 2023/2024 (%)"),
        "trilingual_literacy_pct": _pick(
            demog,
            "Part des personnes sachant lire et écrire l'arabe, le français et l'anglais parmi les alphabètes de 10 ans et plus (%)",
        ),
    }
    return {k: v for k, v in priors.items() if v is not None}


def load_arrondissement_population() -> pd.DataFrame:
    """Load per-arrondissement 2024 population (HCP portal screenshot)."""
    return pd.read_csv(POP_CSV)


if __name__ == "__main__":
    priors = load_province_priors()
    print("HCP 2024 province priors for Préfecture de Casablanca:")
    for k, v in priors.items():
        print(f"  {k:35s} {v}")
    pop = load_arrondissement_population()
    print(f"\nPer-arrondissement population: {len(pop)} zones, total={pop['population_2024'].sum():,}")
