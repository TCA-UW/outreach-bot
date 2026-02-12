import os
import time
import googlemaps
from db_connect import supabase
from dotenv import load_dotenv

load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLEMAPS_PLACES_KEY"))

location = (47.65673397183744, -122.30658974412395)  #uw
radius = 32186  #meters, = 20 miles

keywords = [
    # "accounting","airport","amusement_park","aquarium","art_gallery","atm","bakery","bank",
    # "bar","beauty_salon","bicycle_store","book_store","bowling_alley","bus_station","cafe","campground",
    # "car_dealer","car_rental","car_repair","car_wash","casino",
    # "cemetery","city_hall",
    # "clothing_store","convenience_store","courthouse","dentist","department_store","doctor","drugstore",
    # "electrician","electronics_store","embassy","florist","funeral_home","furniture_store",
    # "gas_station","gym","hair_care","hardware_store","home_goods_store","hospital",
    # "insurance_agency","jewelry_store","laundry","lawyer","library","liquor_store",
    # "local_government_office","locksmith","lodging","meal_delivery","meal_takeaway","movie_rental",
    # "movie_theater","moving_company","museum","painter","park","pet_store","pharmacy",
    # "physiotherapist","plumber","post_office","primary_school","real_estate_agency","restaurant",
    # "roofing_contractor","rv_park","school","secondary_school","shoe_store","shopping_mall","spa","stadium",
    # "storage","store","supermarket","tourist_attraction","travel_agency",
    "university","veterinary_care","zoo"
]

seen_place_ids = set()
inserted = 0
skipped = 0
no_website = 0

for keyword in keywords:
    print(f"\nSearching for: {keyword}")
    try:
        places_result = gmaps.places_nearby(location=location, radius=radius, keyword=keyword)
    except Exception as e:
        print(f"[ERROR] Initial request for {keyword}: {e}")
        continue

    all_results = places_result.get("results", [])

    while 'next_page_token' in places_result:
        time.sleep(2)  
        try:
            places_result = gmaps.places_nearby(
                location=location,
                radius=radius,
                keyword=keyword,
                page_token=places_result['next_page_token']
            )
            all_results.extend(places_result.get("results", []))
        except Exception as e:
            print(f"[ERROR] Paging {keyword}: {e}")
            break

    for place in all_results:
        place_id = place.get("place_id")
        if place_id in seen_place_ids:
            skipped += 1
            continue
        seen_place_ids.add(place_id)

        name = place.get("name")
        # types_list = place.get("types", [])
        # types = ", ".join(types_list)

        try:
            time.sleep(1)
            details = gmaps.place(place_id=place_id).get("result", {})
            description = details.get("formatted_address", "")
            website = details.get("website")
        except Exception as e:
            print(f"[ERROR] {name}: {e}")
            continue

        if not website:
            print(f"[SKIP - no website] {name}")
            no_website +=1
            continue

        try:
            existing = supabase.table("companies").select("company_name").eq("company_name", name).execute()
            if existing.data:
                print(f"[SKIP - already in DB] {name}")
                skipped += 1
                continue

        except Exception as e:
            print(f"[ERROR checking DB] {name}: {e}")
            continue

        try:
            supabase.table("companies").insert({
                'company_name': name,
                'description': description,
                'website': website
            }).execute()
            print(f"[ADDED] {name}")
            inserted += 1
        except Exception as e:
            print(f"[ERROR inserting] {name}: {e}")

print(f"\nInserted {inserted}, Skipped {skipped}, No Website {no_website}")