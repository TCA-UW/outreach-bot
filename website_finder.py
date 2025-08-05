import os
import time
import googlemaps
from dotenv import load_dotenv
from db_connect import supabase

load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLEMAPS_PLACES_KEY"))

response = supabase.table("companies").select("company_id, company_name, description, website, place_id").execute()
companies = response.data

updated = 0
skipped = 0

for company in companies:
    company_id = company["company_id"]
    name = company.get("company_name", "")
    description = company.get("description", "")
    existing_website = company.get("website")
    place_id = company.get("place_id")

    if existing_website:
        print(f"[SKIP - already has website] {name}")
        skipped += 1
        continue

    try:
        if not place_id:
            # If place_id is not already stored, search for it
            query = f"{name} {description}"
            search_result = gmaps.find_place(
                input=query,
                input_type="textquery",
                fields=["place_id"],
                location_bias=f"circle:30000@47.65673397183744,-122.30658974412395"  # 30km around UW
            )
            candidates = search_result.get("candidates", [])
            if not candidates:
                print(f"[NOT FOUND] {name}")
                skipped += 1
                continue

            place_id = candidates[0]["place_id"]
            update_place_id = True
        else:
            update_place_id = False

        # Get place details using place_id
        time.sleep(1)  
        details = gmaps.place(place_id=place_id, fields=["website"]).get("result", {})
        website = details.get("website")

        if website:
            update_data = {"website": website}
            if update_place_id:
                update_data["place_id"] = place_id

            supabase.table("companies").update(update_data).eq("company_id", company_id).execute()
            print(f"[UPDATED] {name} — {website}")
            updated += 1
        else:
            print(f"[NO WEBSITE] {name}")
            skipped += 1

    except Exception as e:
        print(f"[ERROR] {name}: {e}")
        skipped += 1

print(f"\n✅ Done. Updated: {updated}, Skipped: {skipped}")
