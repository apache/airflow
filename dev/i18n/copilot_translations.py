#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
GitHub Copilot API client for translation services.

This module provides a class-based interface to the GitHub Copilot API
with on-demand token refresh and translation capabilities.
"""

from __future__ import annotations

import json
import sys
import threading
import time
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout
from rich import print
from rich.console import Console

if TYPE_CHECKING:
    from jinja2 import Template


COPILOT_CLIENT_ID = "Iv1.b507a08c87ecfe98"
# Create a focused prompt for translation with explicit Unicode handling
LANGUAGE_NAMES = {
    "ar": "Arabic (العربية)",
    "ca": "Catalan (Català)",
    "de": "German (Deutsch)",
    "es": "Spanish (Español)",
    "fr": "French (Français)",
    "he": "Hebrew (עברית)",
    "hi": "Hindi (हिन्दी)",
    "hu": "Hungarian (Magyar)",
    "it": "Italian (Italiano)",
    "ja": "Japanese (日本語)",
    "ko": "Korean (한국어)",
    "nl": "Dutch (Nederlands)",
    "pl": "Polish (Polski)",
    "pt": "Portuguese (Português)",
    "tr": "Turkish (Türkçe)",
    "zh-CN": "Simplified Chinese (简体中文)",
    "zh-TW": "Traditional Chinese (繁體中文)",
}
TODO_PREFIX = "TODO: translate:"


class CopilotTranslator:
    """
    A GitHub Copilot API client with on-demand token refresh and translation capabilities.

    This class handles authentication, token management, and provides methods for
    translating JSON translation files using GitHub Copilot. Tokens are refreshed
    automatically when they expire or become invalid.
    """

    def __init__(self, console: Console | None = None) -> None:
        """Initialize the CopilotTranslator."""
        self.access_token_file = Path(".copilot_token")
        self.access_token: str | None = None
        self.max_retries = 3
        self.token: str | None = None
        self.token_lock = threading.Lock()
        self.console = console or Console(force_terminal=True, color_system="auto")

        # Requests session for connection pooling
        self.session = requests.Session()

        # Set up Jinja2 environment for prompt templates
        self.prompts_dir = Path(__file__).parent / "prompts"
        self.jinja_env = Environment(
            loader=FileSystemLoader(
                [
                    str(self.prompts_dir),  # For global.jinja2
                    str(self.prompts_dir / "locales"),  # For language-specific templates
                ]
            ),
            autoescape=False,
        )
        self.template_cache: dict[str, Template] = {}

    def setup_authentication(self) -> None:
        """Set up GitHub device authentication flow.

        This method initiates the OAuth device flow for GitHub authentication.
        The user will need to visit the provided URL and enter the user code.
        """
        self.console.print("[yellow]Setting up GitHub authentication...[/yellow]")

        resp = requests.post(
            "https://github.com/login/device/code",
            headers={
                "accept": "application/json",
                "editor-version": "Neovim/0.6.1",
                "editor-plugin-version": "copilot.vim/1.16.0",
                "content-type": "application/json",
                "user-agent": "GithubCopilot/1.155.0",
                "accept-encoding": "gzip,deflate,br",
            },
            json={"client_id": COPILOT_CLIENT_ID, "scope": "read:user"},
        )

        resp_json = resp.json()
        device_code = resp_json.get("device_code")
        user_code = resp_json.get("user_code")
        verification_uri = resp_json.get("verification_uri")

        self.console.print(
            f"[bold cyan]Please visit {verification_uri} and enter code {user_code} to authenticate.[/bold cyan]"
        )
        # Wait until the user completes authentication, so wait until user presses Enter
        # with a 1-minute timeout
        if not self._wait_for_user_input_with_timeout(
            "\n\nPress Enter after completing authentication...", timeout=60
        ):
            self.console.print("[red]Authentication timed out. Please try again.[/red]")
            sys.exit(1)

        for _ in range(3):
            time.sleep(5)
            resp = requests.post(
                "https://github.com/login/oauth/access_token",
                headers={
                    "accept": "application/json",
                    "editor-version": "Neovim/0.6.1",
                    "editor-plugin-version": "copilot.vim/1.16.0",
                    "content-type": "application/json",
                    "user-agent": "GithubCopilot/1.155.0",
                    "accept-encoding": "gzip,deflate,br",
                },
                json={
                    "client_id": COPILOT_CLIENT_ID,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
            )

            resp_json = resp.json()
            access_token = resp_json.get("access_token")

            if access_token:
                break

        if not access_token:
            self.console.print("[red]Authentication failed or timed out.[/red]")
            return

        # Save the access token to file
        with open(self.access_token_file, "w") as f:
            f.write(access_token)

        self.console.print("[green]Authentication successful![/green]")

    def _wait_for_user_input_with_timeout(self, prompt: str, timeout: int = 60) -> bool:
        """Wait for user input with a timeout.

        :param prompt: The prompt message to display.
        :param timeout: Timeout in seconds (default: 60).
        :return: True if user pressed Enter within timeout, False if timed out.
        """
        import select

        self.console.print(f"[yellow]{prompt}[/yellow]")

        # Use different approaches based on platform capabilities
        try:
            # For Unix-like systems (macOS, Linux)
            if hasattr(select, "select"):
                ready, _, _ = select.select([sys.stdin], [], [], timeout)
                if ready:
                    sys.stdin.readline()  # Consume the input
                    return True
                self.console.print(
                    f"[red]Timeout: No input received within {timeout} seconds. Continuing...[/red]"
                )
                return False
            # Fallback for systems without select (shouldn't happen on macOS)
            import signal

            def timeout_handler(signum, frame):
                raise TimeoutError("Input timeout")

            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)

            try:
                input()
                signal.alarm(0)  # Cancel the alarm
                return True
            except TimeoutError:
                self.console.print(
                    f"[red]Timeout: No input received within {timeout} seconds. Continuing...[/red]"
                )
                return False

        except Exception as e:
            self.console.print(
                f"[yellow]Warning: Could not set up timeout, using regular input: {e}[/yellow]"
            )
            input(prompt)
            return True

    def _get_access_token(self) -> str:
        """Get the GitHub access token from file or initiate authentication.

        :return: The GitHub access token.
        """
        if not self.access_token_file.exists():
            self.setup_authentication()

        with open(self.access_token_file) as f:
            return f.read().strip()

    def _get_token(self) -> None:
        """Get a fresh Copilot session token using the access token.

        This method exchanges the GitHub access token for a Copilot session token.
        """
        with self.token_lock:
            access_token = self._get_access_token()

            resp = requests.get(
                "https://api.github.com/copilot_internal/v2/token",
                headers={
                    "authorization": f"token {access_token}",
                    "editor-version": "Neovim/0.6.1",
                    "editor-plugin-version": "copilot.vim/1.16.0",
                    "user-agent": "GithubCopilot/1.155.0",
                },
            )

            if resp.status_code == 200:
                resp_json = resp.json()
                self.token = resp_json.get("token")
            else:
                self.console.print(f"[red]Failed to get token: {resp.status_code} {resp.text}[/red]")

    def _is_token_invalid(self, token: str | None) -> bool:
        """Check if the token is invalid or expired.

        :param token: The token to check.
        :return: True if the token is invalid or expired, False otherwise.
        """
        if token is None or "exp" not in token:
            return True

        exp_value = self._extract_exp_value(token)
        return exp_value is None or exp_value <= time.time()

    def _extract_exp_value(self, token: str) -> int | None:
        """Extract the expiration value from the token.

        :param token: The token string.
        :return: The expiration timestamp or None if not found.
        """
        try:
            pairs = token.split(";")
            for pair in pairs:
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    if key.strip() == "exp":
                        return int(value.strip())
        except (ValueError, AttributeError):
            pass
        return None

    def _copilot_complete(self, prompt: str, language: str = "json", retries: int = 0) -> str:
        """Get completion from GitHub Copilot API.

        :param prompt: The prompt to send to Copilot.
        :param language: The language context for the completion.
        :param retries: The current number of retries attempted.
        :return: The completion text from Copilot.
        """
        # For retries
        if retries > self.max_retries:
            self.console.print("[red]Exceeded maximum retries for Copilot completion.[/red]")
            return ""
        if retries > 0:
            self.console.print(
                f"[yellow]Retrying Copilot completion (attempt {retries}/{self.max_retries})...[/yellow]"
            )
            time.sleep(2**retries)  # Exponential backoff

        # Ensure we have a valid token
        if self.token is None or self._is_token_invalid(self.token):
            self._get_token()

        if self.token is None:
            return ""

        try:
            resp = requests.post(
                "https://copilot-proxy.githubusercontent.com/v1/engines/copilot-codex/completions",
                headers={"authorization": f"Bearer {self.token}"},
                json={
                    "prompt": prompt,
                    "suffix": "",
                    "max_tokens": 2000,
                    "temperature": 0.1,
                    "top_p": 1,
                    "n": 1,
                    "stop": ["\n"],
                    "nwo": "github/copilot.vim",
                    "stream": True,
                    "extra": {"language": language},
                },
            )
            resp.raise_for_status()
        except (ConnectionError, Timeout, RequestException) as e:
            self.console.print(f"[red]Connection error while contacting Copilot API: {e}[/red]")
            self._copilot_complete(prompt, language, retries + 1)
            return ""
        except HTTPError as e:
            if resp.status_code == 401:
                self.console.print("[yellow]Token expired or invalid, refreshing token...[/yellow]")
                self._get_token()
                return self._copilot_complete(prompt, language, retries + 1)
            self.console.print(f"[red]HTTP error while contacting Copilot API: {e}[/red]")
            return ""
        except Exception as e:
            self.console.print(f"[red]Unexpected error: {e}[/red]")
            return ""

        result = ""
        # Ensure proper UTF-8 decoding of response
        resp.encoding = "utf-8"
        resp_text = resp.text.split("\n")
        # The response text is streamed line by line the following format:
        # data: {"choices":[{"index":0,"finish_reason":null}]}
        # data: {"choices":[{"text":"變","index":0,"finish_reason":null}]}
        # data: {"choices":[{"text":"數","index":0,"finish_reason":"stop"}]}
        # data: [DONE]

        for line in resp_text:
            if line.startswith("data: {"):
                try:
                    # Parse the JSON part after "data: "
                    json_completion = json.loads(line[6:])
                    completion = json_completion.get("choices", [{}])[0].get("text", "")
                    if completion:
                        result += completion
                    else:
                        result += "\n"
                except json.JSONDecodeError:
                    self.console.print(
                        f"[red]Error decoding JSON response from Copilot API, line : `{line}`[/red]"
                    )
                    continue

        return result.strip()

    def translate(self, lang_path: Path) -> None:
        """Translate a JSON translation file using GitHub Copilot.

        This method loads a JSON translation file, identifies TODO entries,
        and uses GitHub Copilot to provide translations for them.

        :param lang_path: Path to the JSON translation file to translate.
        """

        if not lang_path.exists():
            self.console.print(f"[red]Error: Translation file {lang_path} does not exist[/red]")
            return

        # Determine target language from path
        # Assuming path structure like .../locales/de/common.json
        parts = lang_path.parts
        if "locales" in parts:
            locale_index = parts.index("locales")
            if locale_index + 1 < len(parts):
                target_language = parts[locale_index + 1]
            else:
                target_language = "unknown"
        else:
            target_language = "unknown"

        self.console.print(f"[cyan]Loading translation file: {lang_path}[/cyan]")

        try:
            with open(lang_path, encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            self.console.print(f"[red]Error loading JSON file: {e}[/red]")
            return

        # Find and translate TODO entries
        translations_made = 0
        translations_made = self._translate_recursive(data, target_language, lang_path.stem)

        if translations_made > 0:
            # Save the updated file
            with open(lang_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.write("\n")  # Ensure newline at end

            self.console.print(
                f"[green]Successfully translated {translations_made} entries in {lang_path}[/green]"
            )
        else:
            self.console.print(f"[yellow]No TODO entries found to translate in {lang_path}[/yellow]")

    def _translate_recursive(self, obj: dict, target_language: str, context: str) -> int:
        """Recursively find and translate TODO entries in a JSON structure.

        :param obj: The JSON dictionary object to process.
        :param target_language: The target language code.
        :param context: Context for translation (file name, section, etc.).
        :return: Number of translations made.
        """
        translations_made = 0

        for key, value in obj.items():
            if isinstance(value, str) and value.strip().startswith(TODO_PREFIX):
                # Extract the original text
                original_text = value.replace(TODO_PREFIX, "").strip()
                if original_text:
                    translation = self._get_translation(original_text, target_language, key, context)
                    if translation:
                        obj[key] = translation
                        translations_made += 1
                        self.console.print(
                            f"[green]✓[/green] {context}.{key}: `{original_text}` → `{translation}`"
                        )
                    else:
                        self.console.print(f"[yellow]⚠[/yellow] Failed to translate: {key}")
            elif isinstance(value, dict):
                translations_made += self._translate_recursive(value, target_language, f"{context}.{key}")

        return translations_made

    def _get_translation(self, text: str, target_language: str, key: str, context: str) -> str | None:
        """Get translation for a specific text using GitHub Copilot.

        :param text: The text to translate.
        :param target_language: The target language code.
        :param key: The JSON key for context.
        :param context: Additional context information.
        :return: The translated text or None if translation failed.
        """
        # Get language name for display
        language_name = LANGUAGE_NAMES.get(target_language, target_language)
        # Load global prompt template
        if global_template := self._load_prompt_template("global"):
            prompt = global_template.render(
                language_name=language_name, text=text, key=key, context=context
            ).strip()
            prompt += "\n\n"
        else:
            self.console.print("[red]Error: global.jinja2 template not found.[/red]")
            return None

        # Append language-specific prompt if available
        if language_specific_prompt := self._load_prompt_template(target_language):
            prompt += language_specific_prompt.render().strip()

        try:
            translation = self._copilot_complete(prompt, "text")

            # Clean up the response and ensure proper Unicode handling
            if translation:
                # Remove quotes and clean up
                translation = translation.strip()
                # Remove surrounding quotes if present
                if translation.startswith('"') and translation.endswith('"'):
                    translation = translation[1:-1]
                elif translation.startswith("'") and translation.endswith("'"):
                    translation = translation[1:-1]

                # Ensure proper Unicode decoding if needed
                try:
                    # Try to decode if it's been double-encoded
                    if isinstance(translation, str) and "\\u" in translation:
                        translation = translation.encode("utf-8").decode("unicode_escape")
                except (UnicodeDecodeError, UnicodeEncodeError):
                    pass  # Keep original if decoding fails

                # Basic validation - ensure it's not empty and doesn't look like code
                if translation and not translation.startswith("//") and len(translation.strip()) > 0:
                    # Final Unicode normalization
                    translation = unicodedata.normalize("NFC", translation)
                    return translation.strip()

        except Exception as e:
            self.console.print(f"[red]Translation error for '{text}': {e}[/red]")

        return None

    def _load_prompt_template(self, template_name: str) -> Template | None:
        """Load language-specific or global prompt template.

        :param template_name: The name of the template file (e.g., 'zh-TW', 'de' or 'global').
        :return: The Jinja2 template object or None if template not found.
        """
        if template_name in self.template_cache:
            return self.template_cache[template_name]

        try:
            # Try to load language-specific template (e.g., zh-TW.jinja2) or global.jinja2
            template = self.jinja_env.get_template(f"{template_name}.jinja2")
            self.template_cache[template_name] = template
            return template

        except TemplateNotFound:
            return None
        except Exception as e:
            self.console.print(
                f"[yellow]Warning: Error loading template {template_name}.jinja2: {e}[/yellow]"
            )
            return None


# Example usage if we want to run this script directly
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run dev/i18n/copilot_translations.py <path_to_translation_file>")
        sys.exit(1)

    lang_path = Path(sys.argv[1])
    if not lang_path.exists():
        print(f"Error: File {sys.argv[1]} does not exist.")
        sys.exit(1)

    translator = CopilotTranslator()
    translator.translate(lang_path)
