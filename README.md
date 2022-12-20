dbt model definitions for use with Segment's "Profiles Sync" product.

Profiles Sync lands `_update` tables, the complete historical record of your identity graph. But in many cases, you will only want to use your identity graph's most recent state. The models in this repo are designed to help you produce that.

Models are warehouse-agnostic: Profiles Sync supports Snowflake, BigQuery, and Redshift, and so does this code.


### About Profiles Sync
Profiles Sync lands data non-destructively in your warehouse, giving you access to the complete history of merges, traits, and external ID associations. This allows you to monitor identity health, and reconstruct a prior state of a profile (e.g. prior to a merge).

For many practical use cases, you will want to specifically materialize and use your identity graph's "current state", which would consist of:

- `id_graph`, a lookup table for identities - i.e. where you can join using `segment_id` to combine events that then resolve to a single `canonical_segment_id`. (Derived from the `id_graph_updates` table)
- `external_id_mapping`, a lookup table for external identifiers, e.g. `user_id`,`email`, etc. (Derived from the `external_id_mapping_updates` table)
- `profile_traits`, a table of people and traits - one row per `canonical_segment_id`. Columns in this table are automatically populated from the columns in your `identifies` table. `profile_traits` will include merged-away profiles as well (we don't delete rows). If a profile is merged into another profile, all trait values will be set to NULL, and we will popluate a segment_id in the `merged_to` column. (Derived from the `identifies` table)


For convenience, this package will produce (or "materialize") all three of those models.

### Configuration

`dbt_project.yml` contains 3 main configuration variables:

1. `schema_name`: name of Personas space/schema where the base profiles tables are landing, and these models will read from. Defaults to `identified_events`

2. `etl_overlap`: a "lookback window", in hours used in computing incremental materializations (since different tables can land data at different times). Should be set based on how frequently you are running your materializations - this interval should span 1-2 previous cycles (e.g. if you build every 24 hrs, we might suggest setting this to 49 hrs).

2. `materialization`: means of materializing above views. There is some tradeoff between complexity and efficiency.

	(a) `incremental` (default): most efficient use of computation, incrementally-materialized views

	(b) `table`: less efficient, but also less-complicated materialized tables - each model will be rebuilt from scratch each time.

	(c) `view`: (non-materialized) view definitions which can also be leveraged if you want to avoid DBT orchestration entirely - simply `dbt run` (locally) a single time to establish the definitions (note that you will need to re-run if new traits are instrumented, to add those traits as new columns)


### Installation

You can download this package, standalone, update your configuration as per the section above, and `poetry install`, `poetry shell` and `dbt run`.

You will need to have env vars as specified in `profiles.yml` to run this successfully

You can also import this dbt package as a module in an existing project. dbt offers excellent (albeit slightly outdated) documentation on how that works: https://www.getdbt.com/blog/installing-dbt-packages/

The steps for module import are:

1. Specify the package URL in your `pacakges.yml` file (should be in the main directory of your dbt project; you may need to create the file if it doesn't exist)
OR
Clone the directory and specify the local directory where you have cloned it (since the repo isn't currently public, this may save some headache)

2. Copy the requisite bits of configuration from this package's [`dbt_profiles.yml`] (https://github.com/segmentio/profiles-sync-dbt/blob/main/dbt_project.yml) to your project's `dbt_profiles.yml`. Specifically, you'll want to add the `profiles_sync` sections below to the `vars` and `models` sections of your own `dbt_profiles.yml`.

```
vars:
  profiles_sync:
    schema_name: profiles_v1 # replace this with the name of the schema where Profiles Sync is landing its source tables
    etl_overlap: 28 # should be set to an interval (in hours) that's a bit larger than your materialization cadence

models:
  profiles_sync:
    +on_schema_change: "append_new_columns"
    profile_materializations:
      +materialized: incremental

```