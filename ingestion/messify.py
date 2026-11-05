#yeah bruv the prof wants the data to be "bad" so we have work to do so uh this file takes that shit to the next level
#i uh did not write this shit myself, sadly, i think if i tried i could copy it from some existing repo
#but uh the temptation to use claude code was too strong, so uh, let the record show that i gave in at this instance
#for the record, claude code took like 12s to write all this, not like minutes, so uh yeah that's what i've got in my defense

import os
import random
from datetime import datetime

MESSY_MODE = os.getenv("MESSY_MODE", "1") != "0"
rng = random.Random(int(os.getenv("MESSY_SEED", "42")))

DUPLICATE_EXACT = 0.03
DUPLICATE_CONFLICT = 0.01      # same key, different value
NAME_VARIANT = 0.05
SOURCE_VARIANT = 0.03
DATE_REFORMAT = 0.18           # total, split below
VALUE_NULL = 0.02
VALUE_JUNK_STRING = 0.015
VALUE_SENTINEL = 0.01
VALUE_OUTLIER = 0.005
VALUE_FORMATTED = 0.04
SHUFFLE_WINDOW = 500

JUNK = ["N/A", "n/a", "null", "NULL", "", ".", "--", "NaN", "missing"]
SENTINELS = ["-999", "-9999", "0", "999999"]


def _name_variant(name):
    return rng.choice([
        name.lower(),
        name.upper(),
        f" {name}",
        f"{name} ",
        name.replace(" ", "_"),
        name.replace(" ", ""),
        name.replace("&", "and"),
    ])


def _source_variant(source):
    return rng.choice([source.upper(), source.capitalize(), f"{source}_api", f" {source}"])


def _reformat_date(raw):
    try:
        d = datetime.strptime(str(raw)[:10], "%Y-%m-%d")
    except ValueError:
        return raw
    return rng.choice([
        d.strftime("%m/%d/%Y"),        # 01/05/2024 (US, ambiguous with DD/MM)
        d.strftime("%m/%d/%Y"),
        d.strftime("%d-%b-%Y"),        # 05-Jan-2024
        d.strftime("%Y%m%d"),          # 20240105
        d.strftime("%Y/%m/%d"),
        d.strftime("%d.%m.%Y"),        # 05.01.2024 (EU)
        d.strftime("%B %d, %Y"),       # January 05, 2024
        d.strftime("%Y-%m-%dT%H:%M:%SZ"),
        rng.choice(["TBD", "", "unknown", "0000-00-00"]),   # unparseable
    ])


def _mutate_value(value):
    r = rng.random()
    if r < VALUE_NULL:
        return None
    r -= VALUE_NULL
    if r < VALUE_JUNK_STRING:
        return rng.choice(JUNK)
    r -= VALUE_JUNK_STRING
    if r < VALUE_SENTINEL:
        return rng.choice(SENTINELS)
    r -= VALUE_SENTINEL
    if r < VALUE_OUTLIER:
        try:
            f = float(value)
        except (TypeError, ValueError):
            return value
        return str(rng.choice([f * 100, f / 100, -abs(f), f * 1000]))
    r -= VALUE_OUTLIER
    if r < VALUE_FORMATTED:
        try:
            f = float(value)
        except (TypeError, ValueError):
            return value
        return rng.choice([f"{f:,.2f}", f" {value} ", f"${f:.2f}", f"{f:.2f}%", f"{f:e}"])
    return value


def messify(record):
    """Return a list of records (0..2) derived from one clean-ish record."""
    if not MESSY_MODE:
        return [record]

    rec = dict(record)
    if rng.random() < NAME_VARIANT:
        rec["indicator"] = _name_variant(rec["indicator"])
    if rng.random() < SOURCE_VARIANT:
        rec["source"] = _source_variant(rec["source"])
    if rng.random() < DATE_REFORMAT:
        rec["date"] = _reformat_date(rec["date"])
    rec["value"] = _mutate_value(rec["value"])

    out = [rec]
    r = rng.random()
    if r < DUPLICATE_EXACT:
        out.append(dict(rec))
    elif r < DUPLICATE_EXACT + DUPLICATE_CONFLICT:
        dup = dict(rec)
        try:
            dup["value"] = str(round(float(rec["value"]) * rng.uniform(0.97, 1.03), 4))
        except (TypeError, ValueError):
            pass
        dup["ingested_at"] = datetime.utcnow().isoformat()
        out.append(dup)
    return out


def shuffle_buffer(buffer):
    """Shuffle in windows so records arrive out of order (not globally)."""
    if not MESSY_MODE:
        return buffer
    result = []
    for i in range(0, len(buffer), SHUFFLE_WINDOW):
        chunk = buffer[i:i + SHUFFLE_WINDOW]
        rng.shuffle(chunk)
        result.extend(chunk)
    return result