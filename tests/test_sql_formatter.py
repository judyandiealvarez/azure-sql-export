import os
import unittest

from azure_sql_web import app


TEST_DIR = os.path.dirname(os.path.dirname(__file__))


class SqlFormatterTestCase(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def _read(self, name: str):
        path = os.path.join(TEST_DIR, name)
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    def test_format_matches_expected(self):
        src_sql = self._read('test.sql')
        expected_sql = self._read('shouldbe.sql')

        data = {
            'sql_text': src_sql,
            'keyword_case': 'upper',
            'indent_width': '4',
            'reindent': 'on',
            'space_around_operators': 'on',
        }

        resp = self.client.post('/api/format', data=data)
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        formatted = resp.get_json().get('formatted_sql', '')

        # Normalize trailing newline for comparison
        if not expected_sql.endswith('\n'):
            expected_sql += '\n'

        self.assertEqual(formatted, expected_sql)


if __name__ == '__main__':
    unittest.main()



