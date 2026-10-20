import pytest

from parser import is_single_statement, parse_supported_query, parse_table


def test_parse_table_from_clause():
    assert parse_table("SELECT * FROM orders WHERE cust_id = 42") == "orders"


def test_parse_table_is_case_insensitive():
    assert parse_table("select * from Customers") == "customers"


def test_parse_table_supports_other_statement_forms():
    assert parse_table("INSERT INTO orders VALUES (1, 2)") == "orders"
    assert parse_table("UPDATE customers SET name = 'A'") == "customers"
    assert parse_table(
        "SELECT * FROM orders JOIN customers ON orders.cust_id = customers.cust_id"
    ) == "orders"


def test_parse_table_returns_none_without_a_table():
    assert parse_table("SELECT 1") is None

def test_parse_supported_query_parameterizes_order_id():
    parsed = parse_supported_query("SELECT * FROM orders WHERE order_id = 123")

    assert parsed.params == (123,)
    assert parsed.sql == "SELECT * FROM orders WHERE order_id = ?"
    assert parsed.where == ("order_id", 123)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM orders; DROP TABLE orders;",
        "SELECT * FROM orders WHERE order_id = 1 OR 1=1",
        "SELECT * FROM orders WHERE order_id = '1' OR '1'='1'",
        "DROP TABLE orders",
    ],
)
def test_parse_supported_query_rejects_injection(sql):
    with pytest.raises(ValueError):
        parse_supported_query(sql)


@pytest.mark.parametrize("sql", [
    "", "  \n\t", "SELECT * FROM orders;", "SELECT -- comment\n * FROM orders",
    "SELECT /* comment */ * FROM orders", "SELECT * FROM orders */",
])
def test_statement_safety_checks_are_consistent(sql):
    assert not is_single_statement(sql)
    with pytest.raises(ValueError, match="Only one SQL statement is allowed"):
        parse_supported_query(sql)


def test_supported_query_preserves_whitespace_normalization():
    sql = "  SELECT  *\n FROM orders\tWHERE ORDER_ID = 0  "
    assert is_single_statement(sql)
    parsed = parse_supported_query(sql)
    assert parsed.sql == "SELECT * FROM orders WHERE ORDER_ID = ?"
    assert parsed.params == (0,)
    assert parsed.where == ("order_id", 0)