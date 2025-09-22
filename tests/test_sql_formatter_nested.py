import unittest

from azure_sql_web import app


NESTED_INPUT = """
select id from (
  select id from t where x in (select x from y)
) q
""".strip()

NESTED_EXPECTED = """
SELECT
    id
FROM (
    SELECT
        id
    FROM t
    WHERE x IN (
        SELECT
            x
        FROM y
    )
) q
""".strip() + "\n"


class SqlFormatterNestedTest(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_nested_format(self):
        data = {
            'sql_text': NESTED_INPUT,
            'keyword_case': 'upper',
            'indent_width': '4',
            'reindent': 'on',
            'space_around_operators': 'off',
        }
        resp = self.client.post('/api/format', data=data)
        self.assertEqual(resp.status_code, 200)
        formatted = resp.get_json()['formatted_sql']
        self.assertEqual(formatted, NESTED_EXPECTED)


if __name__ == '__main__':
    unittest.main()
