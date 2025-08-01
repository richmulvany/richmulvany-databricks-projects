# Databricks notebook source
    # DBTITLE 1,Import Dependencies
    from pyspark.sql.functions import col, explode, coalesce, avg, round

    # COMMAND ----------

    # DBTITLE 1,Load Required Silver Tables
    # Combatant events are written to the silver staging layer by the
    # `silver-transform-events.py` notebook.  The `events_combatant` table
    # contains several event types, including ``combatantinfo`` which
    # carries a snapshot of each player's gear at the start of a pull.
    # These records contain a ``gear`` array with item IDs and item
    # levels.  We read this table along with the fights and player
    # details tables to compute average item level per player.
    combatant_events = spark.read.table(
        "`02_silver.staging`.warcraftlogs_events_combatant"
    ).alias("c")
    fights_df = spark.read.table(
        "`02_silver`.warcraftlogs.f_fights_boss_pulls"
    ).alias("f")
    player_details_df = spark.read.table(
        "`02_silver`.warcraftlogs.f_player_details"
    ).alias("pd")

    # COMMAND ----------

    # DBTITLE 1,Transform
    # Filter to combatantinfo events only.  Other event types in
    # ``events_combatant`` include create, summon and resurrect events which
    # do not carry gear information.
    combat_info = combatant_events.filter(col("type") == "combatantinfo")

    # Explode the gear array to access individual gear slots.  The
    # ``combatantinfo`` schema stores gear as an array of structs.  Each
    # struct should contain an item ID and the item level of the equipped
    # piece.  The exact field names can vary across log versions (e.g.
    # ``ilvl``, ``itemLevel`` or ``item_level``).  We use ``coalesce`` to
    # pick the first non-null value across these possibilities.  If none
    # exist the row is dropped.
    exploded = combat_info.withColumn("gear_item", explode(col("gear")))
    itemised = exploded.withColumn(
        "item_level",
        coalesce(
            col("gear_item.ilvl"),
            col("gear_item.itemLevel"),
            col("gear_item.item_level"),
        ),
    ).filter(col("item_level").isNotNull())

    # Calculate average item level per player per pull.  We retain the
    # report ID, report date, pull number and sourceID (player ID) for
    # joining with fights and player details.  Rounding the average to
    # remove excessive decimals yields a cleaner metric.
    per_player_ilvl = (
        itemised.groupby(
            "report_id", "report_date", "pull_number", col("sourceID").alias("player_id")
        )
        .agg(avg("item_level").alias("avg_item_level"))
        .withColumn("avg_item_level", round(col("avg_item_level"), 1))
    )

    # Join with fights to bring raid, boss and difficulty context.  We
    # join on report_id, report_date and pull_number.  The fights table
    # also carries a ``pull_number`` column, so ensure unique naming on
    # both sides.
    ilvl_with_fights = per_player_ilvl.join(
        fights_df,
        [
            per_player_ilvl.report_id == fights_df.report_id,
            per_player_ilvl.report_date == fights_df.report_date,
            per_player_ilvl.pull_number == fights_df.pull_number,
        ],
        how="inner",
    ).select(
        per_player_ilvl.report_id,
        per_player_ilvl.report_date,
        fights_df.raid_name,
        fights_df.boss_name,
        fights_df.raid_difficulty,
        per_player_ilvl.pull_number,
        per_player_ilvl.player_id,
        per_player_ilvl.avg_item_level,
    )

    # Join with player details to attach name, GUID, class, spec and role.
    ilvl_complete = ilvl_with_fights.join(
        player_details_df,
        ilvl_with_fights.player_id == player_details_df.player_id,
        how="left",
    ).select(
        ilvl_with_fights.report_id,
        ilvl_with_fights.report_date,
        ilvl_with_fights.raid_name,
        ilvl_with_fights.boss_name,
        ilvl_with_fights.raid_difficulty,
        ilvl_with_fights.pull_number,
        ilvl_with_fights.player_id,
        player_details_df.player_guid,
        player_details_df.player_name,
        player_details_df.player_class,
        player_details_df.player_spec,
        player_details_df.player_role,
        ilvl_with_fights.avg_item_level.alias("item_level"),
    )

    # COMMAND ----------

    # DBTITLE 1,Write to Gold
    # Define the output table and write the result.  Using overwrite mode
    # ensures that running the notebook repeatedly will refresh the table
    # without creating duplicates.
    output_table = "03_gold.warcraftlogs.player_item_level"
    ilvl_complete.write.mode("overwrite").saveAsTable(output_table)

    print(f"✅ Player item level table written to {output_table}")
