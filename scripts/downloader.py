import os
import time
from typing import List, Optional
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import json
import fitz
import argparse

from document_api_client import get_access_token, create_document, get_all_documents


SHAREPOINT_URL = "https://aithonsolution-my.sharepoint.com/:f:/g/personal/zeeshan_chawdhary_aithonsolutions_com/EgmzFd-Dfh9Mg8ixem5pNYABcqcqpdL-T-TmGXCn_FsenQ?e=g8dhSY"
# DOWNLOAD_PATH = "data/frameDemo/l0"
DOWNLOAD_PATH = "sharepoint_downloads_playwright"

def _ensureDirectory(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _isAllowedFile(name: str, allowed_exts: List[str]) -> bool:
    lower = name.lower().strip()
    for ext in allowed_exts:
        if lower.endswith(ext):
            return True
    return False


def downloadSharepointFiles(
    sharepointUrl: str,
    downloadPath: str,
    allowedExts: Optional[List[str]] = None,
    headless: bool = True,
) -> None:
    """
    Download files from a SharePoint folder page using Playwright.

    Args:
        sharepointUrl: Public or authenticated SharePoint folder URL.
        downloadPath: Local directory where files will be saved.
        allowedExts: List of file extensions to download (e.g., [".pdf", ".xlsx"]).
        headless: Whether to run the browser in headless mode.
    """

    _ensureDirectory(downloadPath)

    if not allowedExts:
        allowedExts = [
            ".pdf",
            ".xlsx",
            ".xls",
            ".csv",
            ".png",
            ".jpg",
            ".jpeg",
            ".docx",
            ".zip",
        ]

    print("\n" + "#" * 70)
    print("# SHAREPOINT DOWNLOADER")
    print("#" * 70)
    print(f"\nStart time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target URL: {sharepointUrl}")
    print(f"Download folder: {downloadPath}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            print("Navigating to SharePoint URL...")
            page.goto(sharepointUrl, timeout=120000)
            time.sleep(8)
            print(f"Page title: {page.title()}")

            file_elements = page.locator("//div[@role='row']//*[contains(text(), '.')]")
            file_count = file_elements.count()
            print(f"Found {file_count} files (any type)")

            if file_count == 0:
                print("No files detected on the page.")
                return

            files_to_download: List[str] = []
            for i in range(file_count):
                name = file_elements.nth(i).inner_text().strip()
                if _isAllowedFile(name, allowedExts):
                    files_to_download.append(name)

            print(f"Detected {len(files_to_download)} eligible files to download:")
            for name in files_to_download:
                print(f"  - {name}")

            for i, file_name in enumerate(files_to_download):
                print(f"\n[{i + 1}/{len(files_to_download)}] Attempting to download: {file_name}")

                local_file_path = os.path.join(downloadPath, file_name)
                if os.path.exists(local_file_path):
                    print(f"  Skipping '{file_name}' — already exists locally.")
                    continue

                print(f"  → Downloading new file: {file_name}")
                try:
                    file_element = page.locator(f"text={file_name}")
                    file_element.first.click()
                    time.sleep(3)

                    download_selectors = [
                        "button[title='Download']",
                        "button[name='Download']",
                        "button[data-automationid='downloadCommand']",
                        "//span[text()='Download']",
                        "//i[@data-icon-name='Download']/ancestor::button",
                    ]

                    download_clicked = False
                    for selector in download_selectors:
                        try:
                            button = page.locator(selector)
                            if button.count() > 0:
                                print(f"  Found download button via {selector}")
                                with page.expect_download(timeout=60000) as dl_info:
                                    button.first.click()
                                download = dl_info.value
                                file_path = os.path.join(downloadPath, download.suggested_filename)
                                download.save_as(file_path)
                                print(f"  Saved {file_path}")
                                download_clicked = True
                                break
                        except Exception:
                            continue

                    if not download_clicked:
                        print("  Toolbar button not found. Trying right-click menu...")
                        file_element.first.click(button="right")
                        time.sleep(2)

                        context_menu_download = page.locator(
                            "//span[text()='Download'] | //button[@name='Download']"
                        )
                        if context_menu_download.count() > 0:
                            with page.expect_download(timeout=60000) as dl_info:
                                context_menu_download.first.click()
                            download = dl_info.value
                            file_path = os.path.join(downloadPath, download.suggested_filename)
                            download.save_as(file_path)
                            print(f"  Saved {file_path} (via context menu)")
                        else:
                            print("  No download option found in context menu.")

                    time.sleep(5)
                    page.goto(sharepointUrl)
                    time.sleep(6)
                    file_elements = page.locator("//div[@role='row']//*[contains(text(), '.')]")

                except PlaywrightTimeoutError:
                    print(f"  Timeout waiting for {file_name}")
                except Exception as e:
                    print(f"  Error downloading {file_name}: {e}")

            print("\n" + "=" * 60)
            print("DOWNLOAD COMPLETE")
            print("=" * 60)

        except Exception as e:
            print(f"General error: {e}")
        finally:
            browser.close()
            print("Browser closed")


__all__ = ["downloadSharepointFiles"]


def removePdfPassword(pdfPath: str, outputPath: str, password: str) -> bool:
    """
    Remove password protection from a PDF. Returns True if successful, else False.
    """
    try:
        doc = fitz.open(pdfPath)
        if not doc.needs_pass:
            doc.save(outputPath)
            doc.close()
            return True
        if not doc.authenticate(password):
            doc.close()
            print(f"❌ Incorrect password for {os.path.basename(pdfPath)}")
            return False
        doc.save(outputPath, encryption=fitz.PDF_ENCRYPT_NONE)
        doc.close()
        return True
    except Exception as e:
        print(f"❌ Error decrypting {pdfPath}: {e}")
        return False


def removePdfWatermark(pdfPath: str, outputPath: str) -> bool:
    """Remove visible watermark/stamps by cleaning page contents."""
    try:
        doc = fitz.open(pdfPath)
        for page in doc:
            page.clean_contents()
        doc.save(outputPath)
        doc.close()
        return True
    except Exception as e:
        print(f"❌ Failed to remove watermark for {pdfPath}: {e}")
        return False


def postProcessDownloads(
    folder: str,
    removePassword: bool = False,
    removeWatermark: bool = False,
    passwordMapPath: Optional[str] = None,
) -> None:
    """
    Optionally remove PDF passwords and/or watermarks in the given folder.
    Passwords are provided via a JSON file mapping filenames or basename to password.
    """
    password_dict = {}
    if removePassword and passwordMapPath and os.path.exists(passwordMapPath):
        with open(passwordMapPath, "r") as f:
            password_dict = json.load(f)
        print(f"Loaded password map for {len(password_dict)} files.")

    for file_name in os.listdir(folder):
        if not file_name.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder, file_name)
        temp_decrypt_path = os.path.join(folder, f"temp_decrypt_{file_name}")
        temp_watermark_path = os.path.join(folder, f"temp_watermark_{file_name}")

        # Remove password if requested
        if removePassword:
            pw = password_dict.get(file_name) or password_dict.get(os.path.splitext(file_name)[0])
            print(f"🔍 Checking {file_name} for password removal, password={'***' if pw else 'None'}")
            if pw:
                if removePdfPassword(pdf_path, temp_decrypt_path, pw):
                    os.remove(pdf_path)
                    os.rename(temp_decrypt_path, pdf_path)
                    print(f"✅ Successfully decrypted and saved {file_name}")
                elif os.path.exists(temp_decrypt_path):
                    os.remove(temp_decrypt_path)
            else:
                print(f"✗ No password found for {file_name}. Skipping password removal.")

        # Remove watermark if requested
        if removeWatermark:
            print(f"💧 Removing watermark from {file_name}")
            try:
                success = removePdfWatermark(pdf_path, temp_watermark_path)
                if success and os.path.exists(temp_watermark_path):
                    os.remove(pdf_path)
                    os.rename(temp_watermark_path, pdf_path)
                    print(f"✅ Watermark removed and saved: {file_name}")
            except Exception as e:
                print(f"❌ Watermark cleanup failed for {file_name}: {e}")
                if os.path.exists(temp_watermark_path):
                    os.remove(temp_watermark_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SharePoint file downloader and post-processor.")
    parser.add_argument("--url", default=SHAREPOINT_URL, help="SharePoint folder URL (default: SHAREPOINT_URL constant).")
    parser.add_argument("--dest", default=DOWNLOAD_PATH, help="Download destination directory (default: DOWNLOAD_PATH constant).")
    parser.add_argument("--password-map", type=str, help="Optional JSON file mapping PDF names to passwords.")
    parser.add_argument("--remove-password", action="store_true", help="Remove PDF password if password found in map.")
    parser.add_argument("--remove-watermark", action="store_true", help="Remove watermark/stamp on PDF.")
    parser.add_argument("--exts", nargs="*", default=None, help="File extensions to download.")
    parser.add_argument("--headless", action="store_true", default=True, help="Run browser in headless mode (default: True).")
    # parser.add_argument("--headless", action="store_true", help="Run browser headless.")
    args = parser.parse_args()

    downloadSharepointFiles(
        sharepointUrl=args.url,
        downloadPath=args.dest,
        allowedExts=args.exts,
        headless=args.headless,
    )

    if args.remove_password or args.remove_watermark:
        postProcessDownloads(
            folder=args.dest,
            removePassword=args.remove_password,
            removeWatermark=args.remove_watermark,
            passwordMapPath=args.password_map,
        )
    print("\n🚀 Starting API Upload Process...")
    token = get_access_token()
    if token:
        existing_docs = get_all_documents(token)
        uploaded_count = 0
        skipped_count = 0
        for file_name in os.listdir(args.dest):  # ✅ use args.dest instead of DOWNLOAD_PATH
            if not file_name.lower().endswith(".pdf"):
                continue
            if file_name in existing_docs:
                print(f"⚠ Skipping '{file_name}' — already exists in API.")
                skipped_count += 1
                continue
            file_path = os.path.join(args.dest, file_name)
            create_document(file_path, token)
            uploaded_count += 1
        print(f"\n✅ Upload Summary:")
        print(f"  • Uploaded: {uploaded_count}")
        print(f"  • Skipped (already exists): {skipped_count}")
        print(f"  • Total processed: {uploaded_count + skipped_count}")
    else:
        print("❌ Skipping API upload — login failed or token not retrieved.")



