# Example Data

The property-insurance example uses fictional data for two Halvard Insurance
Group subsidiaries. The Spanish source has Spanish column names. The Italian
source has Italian column names and includes semantic traps such as net versus
gross premium and reported versus paid claims. Both countries use EUR and the
same ranges for premium, claim, expense, and loss-ratio values.

Run `examples/generate_country_files.py` after setting `catalog_name` and
`schema_name`. It writes 12 deterministic monthly CSV files per country to
`/Volumes/<catalog>/<schema>/landing/<cc>/`, where `<cc>` is `es` or `it`.
Existing files are not rewritten.

Answer keys in `examples/answer_keys/` identify the intended target for every
generated source column. They support AI-proposal evaluation and deterministic
review setup; they are not used by the generator itself.
