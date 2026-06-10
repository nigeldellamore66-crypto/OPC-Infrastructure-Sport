import great_expectations as gx
import os

# Connexion Postgres (adapte le port si tu l'as changé)
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "sportadmin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "abc")
PG_DB = os.getenv("PG_DB", "sportdata")

CONNECTION_STRING = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Contexte éphémère (zéro config à maintenir)
context = gx.get_context(mode="ephemeral")

# Datasource Postgres
datasource = context.sources.add_postgres(
    name="sportdata_pg",
    connection_string=CONNECTION_STRING,
)

# Table cible : analytics.fct_employee_eligibility
asset = datasource.add_table_asset(
    name="fct_employee_eligibility",
    table_name="fct_employee_eligibility",
    schema_name="analytics",
)

batch_request = asset.build_batch_request()

# Suite d'expectations
context.add_or_update_expectation_suite("sport_data_quality")

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="sport_data_quality",
)

# === Les tests ===

# 1. Volumétrie : 161 employés
validator.expect_table_row_count_to_equal(161)

# 2. Coût par employé cohérent : aurait attrapé le bug x12
validator.expect_column_values_to_be_between(
    "total_cost", min_value=0, max_value=10000
)

# 3. Salaires dans une fourchette raisonnable
validator.expect_column_values_to_be_between(
    "gross_salary", min_value=20000, max_value=100000
)

# 4. Business units valides
validator.expect_column_values_to_be_in_set(
    "business_unit",
    ["Finance", "Support", "Ventes", "Marketing", "R&D"],
)

# 5. employee_id non null
validator.expect_column_values_to_not_be_null("employee_id")

validator.save_expectation_suite(discard_failed_expectations=False)

# Exécute
checkpoint = context.add_or_update_checkpoint(
    name="sport_data_checkpoint",
    validator=validator,
)
result = checkpoint.run()

# Résumé console détaillé
print("\n" + "=" * 60)
print(f"Validation: {'OK' if result['success'] else 'ECHEC'}")
print("=" * 60)
for run_result in result["run_results"].values():
    vr = run_result["validation_result"]
    stats = vr["statistics"]
    print(f"Tests réussis  : {stats['successful_expectations']}/{stats['evaluated_expectations']}")
    print("\nDétail des tests échoués :")
    for res in vr["results"]:
        if not res["success"]:
            exp = res["expectation_config"]
            print(f"  ✗ {exp['expectation_type']}")
            print(f"    Colonne/args : {exp['kwargs']}")
            print(f"    Résultat     : {res['result']}")

# Rapport HTML
context.build_data_docs()
docs_sites = context.get_docs_sites_urls()
if docs_sites:
    print(f"\nRapport HTML : {docs_sites[0]['site_url']}")