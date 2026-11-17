# Sales warehouse

The shop's cleaned-up orders (the [clean-orders example](../clean-orders) writes them to `orders-clean`) loaded
into a Snowflake table for the sales reports. Each order is one row, merged by `order_id`, so an order that changes
changes its row instead of adding another. No personal data goes to the warehouse: a hash of the e-mail address
stands for the customer, so repeat customers can still be counted.

| Line | What happens |
|---|---|
| 1 | Ada's order: a row is added. |
| 2 | Grace's order, not paid yet: a row is added. |
| 3 | An order with no e-mail address: added with no customer key. |
| 4 | Grace's order again, now paid: the same row is changed. |

Try the expressions on the sample, with no Snowflake account needed:

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, make the table and a user to load it with, signed in with a key pair:

```sql
CREATE TABLE SHOP.SALES.ORDERS (ORDER_ID VARCHAR, CUSTOMER_KEY VARCHAR, ITEMS NUMBER, TOTAL NUMBER(12,2),
  CURRENCY VARCHAR(3), PLACED_AT TIMESTAMP_TZ, STATUS VARCHAR);
ALTER USER LOADER SET RSA_PUBLIC_KEY = '<the public key, without its BEGIN and END lines>';
```

Set `SNOWFLAKE_PRIVATE_KEY` to the private key (PEM), put the account identifier in `account`, and create the
topic for rows Snowflake refuses:

```sh
openduct topic create sales-warehouse-refused
openduct pipeline run pipeline.yaml
```
