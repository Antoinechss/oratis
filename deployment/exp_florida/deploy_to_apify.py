"""
Script pour créer et déployer l'actor ExP Florida sur Apify via l'API REST
"""
import requests
import os
from pathlib import Path

API_TOKEN = os.environ.get("APIFY_TOKEN", "")
BASE_URL = "https://api.apify.com/v2"
ACTOR_NAME = "exp-florida-scraper"
ACTOR_TITLE = "ExP Florida Scraper"
ACTOR_DESCRIPTION = "Scrape les agents ExP Realty (Florida) via Playwright et push dans Supabase staging"

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}


def get_or_create_actor():
    """Récupère l'ID de l'actor s'il existe, sinon le crée"""
    response = requests.get(
        f"{BASE_URL}/acts",
        headers=headers,
        params={"my": "true", "limit": 100}
    )

    if response.status_code == 200:
        actors = response.json().get("data", {}).get("items", [])
        for actor in actors:
            if actor.get("name") == ACTOR_NAME:
                print(f"Actor existant trouve: {actor['id']}")
                return actor["id"]

    print("Creation d'un nouvel actor...")
    actor_data = {
        "name": ACTOR_NAME,
        "title": ACTOR_TITLE,
        "description": ACTOR_DESCRIPTION,
        "isPublic": False
    }

    response = requests.post(
        f"{BASE_URL}/acts",
        json=actor_data,
        headers=headers
    )

    if response.status_code == 201:
        actor_id = response.json()["data"]["id"]
        print(f"Actor cree: {actor_id}")
        return actor_id
    else:
        print(f"Erreur creation: {response.status_code}")
        print(response.text)
        return None


def create_source_files_array():
    """Crée un tableau avec tous les fichiers de l'actor"""
    actor_dir = Path(__file__).parent

    files_to_include = [
        ("main.py", "main.py"),
        ("requirements.txt", "requirements.txt"),
        ("Dockerfile", "Dockerfile"),
        (".actor/actor.json", ".actor/actor.json"),
        (".actor/INPUT_SCHEMA.json", ".actor/INPUT_SCHEMA.json"),
    ]

    files_array = []
    for file_path, target_name in files_to_include:
        full_path = actor_dir / file_path
        if full_path.exists():
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()
            files_array.append({
                "name": target_name,
                "content": content
            })
            print(f"  + {file_path}")
        else:
            print(f"  ! Non trouve: {file_path}")

    return files_array


def upload_source_code(actor_id):
    """Upload le code source de l'actor"""
    print(f"\nUpload du code source...")

    files_array = create_source_files_array()

    version_data = {
        "versionNumber": "0.0",
        "sourceType": "SOURCE_FILES",
        "sourceFiles": files_array,
        "buildTag": "latest"
    }

    response = requests.post(
        f"{BASE_URL}/acts/{actor_id}/versions",
        json=version_data,
        headers=headers
    )

    if response.status_code == 403 and "already exists" in response.text:
        print("  Version existe, mise a jour...")
        response = requests.put(
            f"{BASE_URL}/acts/{actor_id}/versions/0.0",
            json=version_data,
            headers=headers
        )

    if response.status_code in [200, 201]:
        print("Version creee/mise a jour")
        return True
    else:
        print(f"Erreur version: {response.status_code}")
        print(response.text)
        return False


def build_actor(actor_id):
    """Lance le build de l'actor"""
    print("\nLancement du build...")

    response = requests.post(
        f"{BASE_URL}/acts/{actor_id}/builds?version=0.0",
        headers=headers
    )

    if response.status_code in [200, 201]:
        build_id = response.json().get("data", {}).get("id")
        print(f"Build lance: {build_id}")
        return build_id
    else:
        print(f"Erreur build: {response.status_code}")
        print(response.text)
        return None


def main():
    print("=" * 50)
    print("Deploiement ExP Florida Scraper sur Apify")
    print("=" * 50)

    actor_id = get_or_create_actor()
    if not actor_id:
        print("\nEchec: impossible de creer l'actor")
        return

    success = upload_source_code(actor_id)
    if not success:
        print("\nEchec: impossible d'uploader le code")
        return

    build_id = build_actor(actor_id)

    print("\n" + "=" * 50)
    print("Deploiement termine!")
    print(f"Actor ID: {actor_id}")
    print(f"URL: https://console.apify.com/actors/{actor_id}")
    if build_id:
        print(f"Build: https://console.apify.com/actors/{actor_id}/builds/{build_id}")
    print("=" * 50)


if __name__ == "__main__":
    main()
