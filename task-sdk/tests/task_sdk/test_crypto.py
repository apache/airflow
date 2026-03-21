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
from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from airflow.sdk.crypto import _NullFernet, _RealFernet, get_fernet
from airflow.sdk.exceptions import AirflowException

from tests_common.test_utils.config import conf_vars


class TestNullFernet:
    def test_decryption_of_invalid_type(self):
        """Should raise ValueError for non-string/bytes input."""
        null_fernet = _NullFernet()
        with pytest.raises(ValueError, match="Expected bytes or str, got <class 'int'>"):
            null_fernet.decrypt(123)  # type: ignore[arg-type]

    def test_encrypt_decrypt_roundtrip(self):
        """Should preserve data through encrypt/decrypt cycle."""
        null_fernet = _NullFernet()
        original = b"test message"
        encrypted = null_fernet.encrypt(original)
        decrypted = null_fernet.decrypt(encrypted)
        assert decrypted == original
        assert encrypted == original


class TestRealFernet:
    def test_encryption(self):
        """Encryption should produce encrypted output different from input."""
        from cryptography.fernet import MultiFernet

        key = Fernet.generate_key()
        real_fernet = _RealFernet(MultiFernet([Fernet(key)]))

        msg = b"secret message"
        encrypted = real_fernet.encrypt(msg)

        assert encrypted != msg
        assert real_fernet.decrypt(encrypted) == msg

    def test_rotate_reencrypt_with_primary_key(self):
        """rotate() should re-encrypt data with the primary key."""
        from cryptography.fernet import MultiFernet

        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        # encrypt with key1 only
        encrypted_with_key1 = Fernet(key1).encrypt(b"rotate test")

        # MultiFernet with key2 as primary, key1 as fallback
        multi = MultiFernet([Fernet(key2), Fernet(key1)])
        real_fernet = _RealFernet(multi)

        # rotate should re-encrypt with key2
        rotated = real_fernet.rotate(encrypted_with_key1)

        # key2 should be able to decrypt
        assert Fernet(key2).decrypt(rotated) == b"rotate test"
        assert rotated != encrypted_with_key1


class TestGetFernet:
    @conf_vars({("core", "FERNET_KEY"): ""})
    def test_empty_key(self):
        get_fernet.cache_clear()
        fernet = get_fernet()

        assert not fernet.is_encrypted
        test_data = b"unencrypted"
        assert fernet.encrypt(test_data) == test_data
        assert fernet.decrypt(test_data) == test_data

    @conf_vars({("core", "FERNET_KEY"): Fernet.generate_key().decode()})
    def test_valid_key_encrypts_data(self):
        """Valid FERNET_KEY should return working encryption."""
        get_fernet.cache_clear()
        fernet = get_fernet()

        assert fernet.is_encrypted
        original = b"sensitive data"
        encrypted = fernet.encrypt(original)
        assert encrypted != original
        assert fernet.decrypt(encrypted) == original

    @conf_vars({("core", "FERNET_KEY"): "invalid-key"})
    def test_invalid_key(self):
        """Invalid FERNET_KEY should raise a AirflowException."""
        get_fernet.cache_clear()
        with pytest.raises(AirflowException, match="Could not create Fernet object"):
            get_fernet()

    def test_multiple_keys(self):
        """Multiple comma separated keys should support key rotation."""
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        # encrypt with key1
        data_encrypted_with_key1 = Fernet(key1).encrypt(b"secret data")

        # get_fernet with both keys (key2 primary, key1 fallback)
        with conf_vars({("core", "FERNET_KEY"): f"{key2.decode()},{key1.decode()}"}):
            get_fernet.cache_clear()
            fernet = get_fernet()

            # decrypt data encrypted with old key1
            assert fernet.decrypt(data_encrypted_with_key1) == b"secret data"
            new_encrypted = fernet.encrypt(b"new")
            assert Fernet(key2).decrypt(new_encrypted) == b"new"

    @conf_vars({("core", "FERNET_KEY"): Fernet.generate_key().decode()})
    def test_caching(self):
        """get_fernet() should return cached instance."""
        get_fernet.cache_clear()

        fernet1 = get_fernet()
        fernet2 = get_fernet()

        assert fernet1 is fernet2
