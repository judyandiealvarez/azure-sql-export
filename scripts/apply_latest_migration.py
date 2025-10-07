import glob
import re
import yaml
import pytds


def main() -> None:
    cfg = yaml.safe_load(open('config.yaml', 'r', encoding='utf-8'))
    paths = sorted(glob.glob('sql/migrations/update*.sql'))
    if not paths:
        print('No migration files found')
        return
    path = paths[-1]
    sql = open(path, 'r', encoding='utf-8', newline='').read()
    batches = re.split(r'(?mi)^\s*GO\s*$', sql)
    with pytds.connect(
        server=cfg['server'],
        database=cfg['database'],
        user=cfg['username'],
        password=cfg['password'],
        port=1433,
        cafile=None,
        validate_host=False,
    ) as conn:
        cur = conn.cursor()
        for b in batches:
            if b.strip():
                cur.execute(b)
    print('Applied', path)


if __name__ == '__main__':
    main()


