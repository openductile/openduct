# QuickBooks Online

Customers and invoices in [QuickBooks Online](https://quickbooks.intuit.com).

## Registering this installation with Intuit (once, by its admin)

1. At [developer.intuit.com](https://developer.intuit.com), **Dashboard → Create an app → QuickBooks Online and
   Payments**, with the scope `com.intuit.quickbooks.accounting`.
2. In the app's **Keys & credentials**, add the redirect URI shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`).
3. Copy the **Client ID** and **Client Secret** into **Settings → App sign-ins → QuickBooks Online**. Use the
   Development keys with a sandbox company; Production keys need Intuit's app assessment.

## Connecting a company

Under Credentials, add a QuickBooks Online credential (or its sandbox one) and **Connect**: sign in at Intuit and
choose the company. OpenDuct keeps the token and the company's id, and renews the token as it runs out.

## Actions

- **Add a customer** (`add_customer`).
- **Make an invoice** (`create_invoice`): its lines in QuickBooks' own form.
- **Find records** (`find`): a QuickBooks query, such as `select * from Invoice where Balance > '0'`.

## Tested

Recorded exchanges from Intuit's documented requests and answers (`tests/`).

Tried on 2026-11-16: the test company's sign-in had run out (Intuit no longer takes its refresh token), so it needs Connect again before it can be run live.
