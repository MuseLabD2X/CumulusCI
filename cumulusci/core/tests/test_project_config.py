import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cumulusci.core.config.project_config import BaseProjectConfig
from cumulusci.core.config.universal_config import UniversalConfig
from cumulusci.core.keychain.base_project_keychain import BaseProjectKeychain
from cumulusci.utils.yaml.cumulusci_yml import cci_safe_load


class TestBaseProjectConfig(unittest.TestCase):
    def setUp(self):
        self.universal_config = UniversalConfig()
        self.project_config = BaseProjectConfig(self.universal_config)
        self.project_config.keychain = BaseProjectKeychain(self.project_config, None)

    def test_load_folder_config(self):
        with tempfile.TemporaryDirectory() as tempdir:
            config_dir = Path(tempdir) / "cumulusci.d"
            config_dir.mkdir()
            (config_dir / ".order.yml").write_text("component1.yml\ncomponent2.yml\n")
            (config_dir / "component1.yml").write_text("key1: value1\n")
            (config_dir / "component2.yml").write_text("key2: value2\n")

            folder_config = self.project_config._load_folder_config()
            self.assertEqual(folder_config["key1"], "value1")
            self.assertEqual(folder_config["key2"], "value2")

    def test_load_folder_config_recursive(self):
        with tempfile.TemporaryDirectory() as tempdir:
            config_dir = Path(tempdir) / "cumulusci.d"
            config_dir.mkdir()
            submodule_dir = config_dir / "submodule"
            submodule_dir.mkdir()
            (config_dir / ".order.yml").write_text("component1.yml\nsubmodule/\n")
            (config_dir / "component1.yml").write_text("key1: value1\n")
            (submodule_dir / ".order.yml").write_text("subcomponent1.yml\n")
            (submodule_dir / "subcomponent1.yml").write_text("key2: value2\n")

            folder_config = self.project_config._load_folder_config()
            self.assertEqual(folder_config["key1"], "value1")
            self.assertEqual(folder_config["key2"], "value2")

    @patch("cumulusci.core.config.project_config.BaseProjectConfig.repo_root", new_callable=PropertyMock)
    def test_load_config_with_folder(self, mock_repo_root):
        with tempfile.TemporaryDirectory() as tempdir:
            mock_repo_root.return_value = tempdir
            config_path = Path(tempdir) / "cumulusci.yml"
            config_path.write_text("project:\n  name: TestProject\n")
            config_dir = Path(tempdir) / "cumulusci.d"
            config_dir.mkdir()
            (config_dir / ".order.yml").write_text("component1.yml\ncomponent2.yml\n")
            (config_dir / "component1.yml").write_text("key1: value1\n")
            (config_dir / "component2.yml").write_text("key2: value2\n")

            self.project_config._load_config()
            self.assertEqual(self.project_config.config_project["project"]["name"], "TestProject")
            self.assertEqual(self.project_config.config_project["key1"], "value1")
            self.assertEqual(self.project_config.config_project["key2"], "value2")

    def test_load_order_file(self):
        with tempfile.TemporaryDirectory() as tempdir:
            order_file = Path(tempdir) / ".order.yml"
            order_file.write_text("component1.yml\ncomponent2.yml\n")
            order = cci_safe_load(order_file)
            self.assertEqual(order, ["component1.yml", "component2.yml"])

    def test_merge_dicts(self):
        base = {"key1": "value1", "key2": {"subkey1": "subvalue1"}}
        new = {"key2": {"subkey2": "subvalue2"}, "key3": "value3"}
        merged = self.project_config._merge_dicts(base, new)
        self.assertEqual(merged["key1"], "value1")
        self.assertEqual(merged["key2"]["subkey1"], "subvalue1")
        self.assertEqual(merged["key2"]["subkey2"], "subvalue2")
        self.assertEqual(merged["key3"], "value3")
