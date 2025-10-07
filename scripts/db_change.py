import yaml
import pytds


def main() -> None:
    cfg = yaml.safe_load(open('config.yaml', 'r', encoding='utf-8'))

    schema = 'BPG_FinOps_Invoice_Reimbursement'
    view = 'c_MultigroupProposalInvoices'

    create_if_missing = (
        f"IF OBJECT_ID('[{schema}].[{view}]','V') IS NULL "
        f"EXEC('CREATE VIEW [{schema}].[{view}] AS SELECT TOP 1 * FROM [{schema}].[TransactionCost]')"
    )
    alter_sql = (
        f"ALTER VIEW [{schema}].[{view}] AS SELECT TOP 2 * FROM [{schema}].[TransactionCost]"
    )

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
        cur.execute(create_if_missing)
        cur.execute(alter_sql)
    print('DB view changed')


if __name__ == '__main__':
    main()


