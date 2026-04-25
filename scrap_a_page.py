import re
import sys
import requests
import anthropic
import yaml
from bs4 import BeautifulSoup

# page_to_scrap = 'https://www.amazon.com/dp/B0GSGZYLF4?tag=synack-20&linkCode=osi&th=1&psc=1'
page_to_scrap = 'https://www.amazon.com/dp/B09GBGDMJN?tag=synack-20&linkCode=osi&th=1&psc=1'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
}

AMAZON_SECTION_IDS = [
    'productTitle',
    'priceblock_ourprice',
    'priceblock_dealprice',
    'corePriceDisplay_desktop_feature_div',
    'feature-bullets',
    'productDetails_techSpec_section_1',
    'productDetails_techSpec_section_2',
    'detailBullets_feature_div',
    'productDescription',
    'acrPopover',
    'averageCustomerReviews',
    'desktop_unifiedPrice',
    'apex_desktop',
    'buybox',
    'merchant-info',
    'tabular-buybox',
    'technicalSpecifications_feature_div',
    'prodDetails',
    'productDetails_db_sections',
]


def fetch_page(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def extract_relevant_sections(html: str) -> str:
    soup = BeautifulSoup(html, 'lxml')

    # Remove noise
    for tag in soup(['script', 'style', 'nav', 'iframe', 'noscript']):
        tag.decompose()

    parts = []

    # Pull targeted sections by ID
    for section_id in AMAZON_SECTION_IDS:
        el = soup.find(id=section_id)
        if el:
            text = el.get_text(separator='\n', strip=True)
            if text:
                parts.append(f'=== {section_id} ===\n{text}')

    # Also grab any table with specs
    for table in soup.find_all('table'):
        text = table.get_text(separator='\t', strip=True)
        if text and len(text) > 50:
            parts.append(f'=== table ===\n{text}')

    # Fallback: full page text if nothing useful found
    if not parts:
        full_text = soup.get_text(separator='\n', strip=True)
        return full_text[:60000]

    combined = '\n\n'.join(parts)
    return combined[:60000]


def strip_yaml_fences(text: str) -> str:
    text = text.strip()
    text = re.sub(r'^```(?:yaml)?\s*\n', '', text)
    text = re.sub(r'\n```\s*$', '', text)
    return text.strip()


def extract_disk_info_with_claude(page_content: str, url: str) -> dict:
    client = anthropic.Anthropic()

    system_prompt = (
        "You are a product data extraction specialist. "
        "Given scraped Amazon page content for a disk drive, you extract every piece of "
        "useful product information and return it as clean, valid YAML. "
        "Never wrap your output in markdown code fences. Output only raw YAML."
    )

    user_prompt = f"""Extract ALL available disk drive product information from the Amazon page content below.

URL: {url}

--- PAGE CONTENT ---
{page_content}
--- END CONTENT ---

Return a single YAML document (no code fences, no markdown) with exaclty the following fields.
Extract exactly one value of each field.
If information could not be found for a field, guess a reasonable value based on the page content.

- product_name (this should be the full product name as listed on the page)
- average_rating (number between 0 and 5, e.g. 4.7)
- total_no_of_reviews (eg: 100, 122, 1023, etc.)
- brand (this should be alphanumeric string)
- model (this should be alphanumeric string)
- price_usd (e.g. 199.99)
- in_stock (options: yes,no,limited)
- condition (options: new, used, refurbished)
- ships_from (example: Amazon, third-party, etc.)
- sold_by (vendor name)
- prime_eligible (options: yes/no)
- drive_type: (options: HDD, SSD, NVMe SSD, Hybrid, Portable HDD, Portable SSD)
- capacity_in_TB: (e.g. 2, 10)
- form_factor: (options: 2.5" / 3.5" / M.2 / mSATA)
- mount_type: (options: internal / external)
- interface: (options: SATA III / NVMe PCIe / USB 3.2 / USB 3.0 / USB 2.0)
- read_speed_mbps: (e.g. 560)
- write_speed_mbps: (e.g. 530)
- estimated_delivery_date: (latest estimated delivery date in format MM/DD)
- warranty_months: (eg: 12)

"""

    with client.messages.stream(
        # model="claude-opus-4-7",
        model="claude-haiku-4-5",
        max_tokens=8192,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        response = stream.get_final_message()

    yaml_text = ""
    for block in response.content:
        if block.type == "text":
            yaml_text = block.text
            break

    yaml_text = strip_yaml_fences(yaml_text)

    try:
        data = yaml.safe_load(yaml_text)
        if isinstance(data, dict):
            return data
        return {"extraction": data}
    except yaml.YAMLError as exc:
        print(f"Warning: Claude returned invalid YAML ({exc}). Saving raw text.", file=sys.stderr)
        return {"raw_extraction": yaml_text}


def save_yaml(data: dict, path: str) -> None:
    with open(path, 'w', encoding='utf-8') as fh:
        yaml.dump(data, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)


def main() -> None:
    url = page_to_scrap
    output_file = 'disk_info.yaml'

    print(f"Fetching: {url}")
    try:
        html = fetch_page(url)
    except requests.RequestException as exc:
        print(f"Failed to fetch page: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Fetched {len(html):,} bytes. Extracting relevant sections…")
    page_content = extract_relevant_sections(html)
    print(f"Extracted {len(page_content):,} characters for Claude.")

    print("Calling Claude API (claude-opus-4-7) for structured extraction…")
    disk_info = extract_disk_info_with_claude(page_content, url)

    print(disk_info)

    print(f"Saving to {output_file}…")
    save_yaml(disk_info, output_file)
    print(f"Done! Product info saved to {output_file}")


if __name__ == "__main__":
    main()
