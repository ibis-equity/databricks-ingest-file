import json
from pathlib import Path
from random import choice, randint

first_names = ["Daryl", "Alicia", "Marcus", "Sofia", "Jason", "Priya", "Noah", "Lena", "Omar", "Chloe"]
last_names = ["Shackleford", "Reynolds", "Lee", "Martinez", "Kim", "Patel", "Nguyen", "Brown", "Garcia", "Lopez"]
cities = ["Charlotte", "Raleigh", "Durham", "Atlanta", "Austin", "Seattle", "Denver", "Chicago", "Boston", "Houston"]
states = ["NC", "GA", "TX", "WA", "CO", "IL", "MA"]
companies = ["Ibis Equity Consulting", "TechNova", "CloudShift", "InsightWorks", "DataForge", "BrightAnalytics"]
roles = ["Technical Director", "Data Engineer", "ML Engineer", "Analytics Lead", "Senior Engineer", "Platform Architect"]
skills_pool = ["Python", "PySpark", "Databricks", "AWS", "Azure", "SQL", "Delta Lake", "RAG", "Kafka", "dbt"]

records = []

for i in range(1, 1001):
    skills = [choice(skills_pool) for _ in range(randint(2, 5))]
    phones = [
        {"type": "mobile", "number": f"555-{randint(100,999)}-{randint(1000,9999)}"}
    ]
    if randint(0, 1):
        phones.append({"type": "work", "number": f"555-{randint(100,999)}-{randint(1000,9999)}"})

    rec = {
        "id": f"{i:03d}",
        "name": {
            "first": choice(first_names),
            "last": choice(last_names)
        },
        "contact": {
            "email": f"user{i}@example.com",
            "phones": phones
        },
        "address": {
            "street": f"{100 + i} Main St",
            "city": choice(cities),
            "state": choice(states),
            "zip": f"{randint(20000, 99999)}"
        },
        "employment": {
            "company": choice(companies),
            "role": choice(roles),
            "skills": skills
        }
    }
    records.append(rec)

# Save as a single JSON file in a Unity Catalog Volume.
volume_dir = "/Volumes/workspace/default/raw_filings"
dbfs_volume_dir = f"dbfs:{volume_dir}"
dbfs_path = f"{dbfs_volume_dir}/people_nested_1000.json"

payload = json.dumps(records)

dbutils_obj = globals().get("dbutils")
if dbutils_obj is not None:
    dbutils_obj.fs.mkdirs(dbfs_volume_dir)
    dbutils_obj.fs.put(dbfs_path, payload, overwrite=True)
    print(f"Wrote {len(records)} records to {dbfs_path}")
else:
    # Local fallback: keep generation usable in IDE/CLI when notebook dbutils is unavailable.
    local_path = Path("people_nested_1000.json").resolve()
    local_path.write_text(payload, encoding="utf-8")
    print(f"dbutils not available; wrote local file instead: {local_path}")
