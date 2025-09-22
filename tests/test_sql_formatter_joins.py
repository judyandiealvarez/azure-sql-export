import unittest

from azure_sql_web import app


JOIN_INPUT = """
select a.id, b.name
from dbo.A a join dbo.B b on b.id=a.id
where a.x=1
""".strip()

JOIN_EXPECTED = """
SELECT
    a.id,
    b.name
FROM dbo.A a
JOIN dbo.B b
    ON b.id=a.id
WHERE a.x=1
""".strip() + "\n"


class SqlFormatterJoinsTest(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_joins_format(self):
        data = {
            'sql_text': JOIN_INPUT,
            'keyword_case': 'upper',
            'indent_width': '4',
            'reindent': 'on',
            'space_around_operators': 'off',
        }
        resp = self.client.post('/api/format', data=data)
        self.assertEqual(resp.status_code, 200)
        formatted = resp.get_json()['formatted_sql']
        self.assertEqual(formatted, JOIN_EXPECTED)


if __name__ == '__main__':
    unittest.main()


