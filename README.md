dbt model definitions for use with Twilio-Segment's **Profiles Sync** feature.

Profiles Sync lands `_update` tables, the complete historical record of your
identity graph. The models in this repo are designed to help you produce your
identity graph's *most recent state*.

This repo is tested for

+ Snowflake
+ BigQuery
+ Redshift


### About Profiles Sync

Profiles Sync lands data non-destructively in your warehouse, giving you access
to the complete history of merges, traits, and external ID associations.
This allows you to monitor identity health, and reconstruct a prior
state of a profile (e.g. prior to a merge).

For many practical use cases, you will want to materialize and use your
identity graph's **current state**.

Specifically, we recommend you create:

| Materialized Table    | Source Table                  | Description                                                                                                                                        |
| ------------------    | --------------                | ------------------------------                                                                                                                     |
| `id_graph`            | `id_graph_updated`            | a lookup table for identities - i.e. where you can join using `segment_id`  to combine events that then resolve to a single `canonical_segment_id` |
| `external_id_mapping` | `external_id_mapping_updates` | a lookup table for external identifiers such as `user_id`, `email`, etc.                                                                           |
| `profile_traits`      | `identifies`                  | a table of people and traits - one row per `canonical_segment_id`.                                                                                 |

`profile_traits` - Columns in this table are automatically populated from the columns of its source table `identifies`.
As we do not delete any rows, this table also includes merged-away profiles with their trait values set to `NULL`.
To identify an older profile, its `merged_to` column will be populated with its latest `segment_id`.

For convenience, this package will produce (or "materialize") all three of those models.

### Configuration

`dbt_project.yml` contains 4 main configuration variables:

1. `profiles`: the name *dbt* parses to fetch warehouse credentials from `profiles.yml`. Refer [dbt docs](https://docs.getdbt.com/docs/get-started/connection-profiles) on how to configure your warehouse connection profiles `profiles.yml`.

2. `schema_name`: name of the Personas space/schema where the base tables land. Defaults to `identified_events`.

3. `etl_overlap`: frequency (in hours) of materialization runs. We recommend this number to span 1-2 previous cycles to account for different tables landing at different times. (e.g. if you build every `24` hrs, we might suggest setting this to `24*2+1=49` hrs).

4. `materialization`: means of materializing above views. Refer [dbt docs: materializations](https://docs.getdbt.com/docs/build/materializations) for more details.
    
    * `incremental` (default): most efficient use of computation, incrementally-materialized views
    * `table`: less efficient, but also less-complicated materialized tables - each table will be rebuilt from scratch each time.
    * `view`: (non-materialized) view definitions which can also be leveraged if you want to avoid dbt orchestration entirely - simply `dbt run` (locally) a single time to establish the definitions (note that you will need to re-run if new traits are instrumented, to add those traits as new columns)


### Installation

You can download this package, standalone, update your configuration as per the section above, and `dbt run`.

You can also import this dbt package as a module in an existing project. dbt offers [excellent (albeit slightly outdated) documentation](https://www.getdbt.com/blog/installing-dbt-packages/) on how that works:

The steps for module import are:

1. Specify the package URL in your `packages.yml` file (should be in the main directory of your dbt project; you may need to create the file if it doesn't exist)
OR 
Clone the directory and specify the local directory where you have cloned it (since the repo isn't currently public, this may save some headache)

2. Copy the requisite bits of configuration from this package's [`dbt_profiles.yml`](https://github.com/segmentio/profiles-sync-dbt/blob/main/dbt_project.yml) to your project's `dbt_profiles.yml`. Specifically, you'll want to add the `profiles_sync` sections below to the `vars` and `models` sections of your own `dbt_profiles.yml`.

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
