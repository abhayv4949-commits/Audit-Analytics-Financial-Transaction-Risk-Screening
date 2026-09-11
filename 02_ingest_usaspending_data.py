# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

import requests
import time

url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

request_body = {
    "subawards": False,
    "limit": 100,
    "page": 1,
    "filters": {
        "award_type_codes": ["A", "B", "C", "D"],
        "agencies": [
            {"type": "awarding", "tier": "toptier", "name": "Department of Defense"}
        ],
        "time_period": [
            {"start_date": "2022-10-01", "end_date": "2024-09-30"}
        ]
    },
    "fields": [
        "Award ID", "Recipient Name", "Start Date", "End Date",
        "Award Amount", "Awarding Agency", "Awarding Sub Agency",
        "Award Type", "Funding Agency", "Funding Sub Agency"
    ]
}

all_results = []
page = 1
max_pages = 200  # safety cap so a bug can't loop forever — ~20,000 records at limit=100

while True:
    request_body["page"] = page
    response = requests.post(url, json=request_body, timeout=30)

    if response.status_code != 200:
        print(f"ERROR on page {page}: {response.status_code} — {response.text[:500]}")
        break

    data = response.json()
    results = data.get("results", [])
    all_results.extend(results)

    has_next = data.get("page_metadata", {}).get("hasNext", False)
    print(f"Page {page} — got {len(results)} records — total so far: {len(all_results)} — hasNext: {has_next}")

    if not has_next or page >= max_pages:
        break

    page += 1
    time.sleep(0.2)  # small delay to be polite to the API

print(f"\nDONE. Total records collected: {len(all_results)}")

# COMMAND ----------

from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("Award ID", T.StringType()),
    T.StructField("Recipient Name", T.StringType()),
    T.StructField("Start Date", T.StringType()),
    T.StructField("End Date", T.StringType()),
    T.StructField("Award Amount", T.DoubleType()),
    T.StructField("Awarding Agency", T.StringType()),
    T.StructField("Awarding Sub Agency", T.StringType()),
    T.StructField("Award Type", T.StringType()),
    T.StructField("Funding Agency", T.StringType()),
    T.StructField("Funding Sub Agency", T.StringType()),
])

df = spark.createDataFrame(all_results, schema=schema)

# Delta doesn't allow spaces in column names — rename to clean snake_case
rename_map = {
    "Award ID": "award_id",
    "Recipient Name": "recipient_name",
    "Start Date": "start_date",
    "End Date": "end_date",
    "Award Amount": "award_amount",
    "Awarding Agency": "awarding_agency",
    "Awarding Sub Agency": "awarding_sub_agency",
    "Award Type": "award_type",
    "Funding Agency": "funding_agency",
    "Funding Sub Agency": "funding_sub_agency",
}
for old_name, new_name in rename_map.items():
    df = df.withColumnRenamed(old_name, new_name)

df.write.format("delta").mode("overwrite").saveAsTable("bronze_usaspending_awards")

print(f"Bronze table written: {df.count()} rows")
df.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_awards = spark.table("bronze_usaspending_awards")

print(f"Bronze row count: {bronze_awards.count()}")

# --- Step 1: see the messiness before you fix it (worth actually looking at) ---
print("\nSample of raw recipient names (notice the inconsistency):")
bronze_awards.select("recipient_name").distinct().orderBy("recipient_name").show(20, truncate=False)

# COMMAND ----------

# Chain: trim -> uppercase -> strip punctuation -> strip common legal suffixes -> collapse spaces
silver_awards = (
    bronze_awards
    .withColumn("recipient_name_clean", F.trim(F.upper(F.col("recipient_name"))))
    .withColumn("recipient_name_clean", F.regexp_replace("recipient_name_clean", r"[\.,]", ""))
    .withColumn(
        "recipient_name_clean",
        F.regexp_replace(
            "recipient_name_clean",
            r"\b(INC|LLC|CORP|CORPORATION|CO|LTD|LP|LLP|PC|PA)\b\s*$",
            ""
        )
    )
    .withColumn("recipient_name_clean", F.trim(F.regexp_replace("recipient_name_clean", r"\s+", " ")))
)

# COMMAND ----------

silver_awards = (
    silver_awards
    .withColumn("start_date_parsed", F.to_date("start_date", "yyyy-MM-dd"))
    .withColumn("end_date_parsed", F.to_date("end_date", "yyyy-MM-dd")))

# COMMAND ----------

silver_awards = (
    silver_awards
    .filter(F.col("award_amount").isNotNull())
    .filter(F.col("award_amount") > 0)
    .filter(F.col("recipient_name_clean") != "")
)

# COMMAND ----------

silver_awards = silver_awards.dropDuplicates()

silver_awards.write.format("delta").mode("overwrite").saveAsTable("silver_usaspending_awards")

print(f"\nSilver row count: {silver_awards.count()}")
print(f"Distinct raw recipient names:   {bronze_awards.select('recipient_name').distinct().count()}")
print(f"Distinct CLEANED recipient names: {silver_awards.select('recipient_name_clean').distinct().count()}")
print("(the second number should be noticeably smaller — that gap IS your cleanup working)")

silver_awards.select("recipient_name", "recipient_name_clean", "award_amount", "start_date_parsed").show(10, truncate=False)

# COMMAND ----------


# Cell 2 — Silver for Module 2: Synthetic ERP data
# =========================================================
# Already clean since it's programmatically generated — but still standardize
# for consistency and to demonstrate the same discipline across both modules.

dim_vendor = spark.table("dim_vendor")
dim_user = spark.table("dim_user")
fact_po = spark.table("fact_purchase_order")
fact_gr = spark.table("fact_goods_receipt")
fact_inv = spark.table("fact_invoice").drop("_is_planted_mismatch")  # drop the answer key — see Section 5.2 of spec
fact_je = spark.table("fact_journal_entry")

# Standardize vendor names the same way, for consistency of method across modules
silver_vendor = (
    dim_vendor
    .withColumn("vendor_name_clean", F.trim(F.upper(F.col("vendor_name"))))
    .withColumn("vendor_name_clean", F.regexp_replace("vendor_name_clean", r"[\.,]", ""))
)

silver_vendor.write.format("delta").mode("overwrite").saveAsTable("silver_dim_vendor")
dim_user.write.format("delta").mode("overwrite").saveAsTable("silver_dim_user")
fact_po.write.format("delta").mode("overwrite").saveAsTable("silver_fact_purchase_order")
fact_gr.write.format("delta").mode("overwrite").saveAsTable("silver_fact_goods_receipt")
fact_inv.write.format("delta").mode("overwrite").saveAsTable("silver_fact_invoice")
fact_je.write.format("delta").mode("overwrite").saveAsTable("silver_fact_journal_entry")

print("Module 2 Silver tables written.")
print(f"silver_fact_invoice columns: {fact_inv.columns}")
print("(confirm '_is_planted_mismatch' is NOT in that list — it shouldn't be, we dropped it)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Cell 1 — Test: Vendor Concentration Risk

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_awards = spark.table("silver_usaspending_awards")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: total spend per cleaned recipient
# MAGIC

# COMMAND ----------

vendor_totals = (
    silver_awards
    .groupBy("recipient_name_clean")
    .agg(
        F.sum("award_amount").alias("total_spend"),
        F.count("*").alias("award_count")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: rank vendors by spend, compute % of total population spend

# COMMAND ----------

total_population_spend = vendor_totals.agg(F.sum("total_spend")).collect()[0][0]

vendor_totals = (
    vendor_totals
    .withColumn("pct_of_total_spend", F.round(F.col("total_spend") / total_population_spend * 100, 2))
    .orderBy(F.desc("total_spend"))
    .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("total_spend"))))
)


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3: the actual concentration risk metric — what % of spend sits with top 5% of vendors?

# COMMAND ----------

total_vendor_count = vendor_totals.count()
top_5_pct_count = max(1, int(total_vendor_count * 0.05))

top_vendors_spend = (
    vendor_totals
    .filter(F.col("rank") <= top_5_pct_count)
    .agg(F.sum("total_spend"))
    .collect()[0][0]
)

concentration_pct = round((top_vendors_spend / total_population_spend) * 100, 2)

print(f"Total vendors in population: {total_vendor_count}")
print(f"Total population spend: ${total_population_spend:,.2f}")
print(f"Top 5% of vendors ({top_5_pct_count} vendors) account for {concentration_pct}% of total spend")
print("\nTop 15 vendors by spend:")
vendor_totals.select("rank", "recipient_name_clean", "total_spend", "award_count", "pct_of_total_spend").show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: write the Gold table — flag vendors above a concentration threshold as high-risk

# COMMAND ----------

gold_vendor_concentration = (
    vendor_totals
    .withColumn(
        "risk_flag",
        F.when(F.col("pct_of_total_spend") >= 5.0, "HIGH — single vendor >5% of total spend")
         .when(F.col("pct_of_total_spend") >= 2.0, "MEDIUM — single vendor >2% of total spend")
         .otherwise("LOW")
    )
)

gold_vendor_concentration.write.format("delta").mode("overwrite").saveAsTable("gold_vendor_concentration")
print("\nGold table 'gold_vendor_concentration' written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2 — Test: Duplicate / Near-Duplicate Awards (Module 1)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_awards = spark.table("silver_usaspending_awards")

# Self-join approach: for each award, find other awards from the SAME vendor
# with a SIMILAR amount (within 2%) awarded within 30 days
w = Window.partitionBy("recipient_name_clean").orderBy("start_date_parsed")

df = silver_awards.withColumn(
    "prev_amount", F.lag("award_amount").over(w)
).withColumn(
    "prev_date", F.lag("start_date_parsed").over(w)
).withColumn(
    "days_since_prev", F.datediff(F.col("start_date_parsed"), F.col("prev_date"))
).withColumn(
    "pct_amount_diff",
    F.when(F.col("prev_amount").isNotNull(),
           F.abs(F.col("award_amount") - F.col("prev_amount")) / F.col("prev_amount") * 100
    )
)

flagged_duplicates = df.filter(
    (F.col("days_since_prev") <= 30) &
    (F.col("pct_amount_diff") <= 2.0) &
    (F.col("prev_amount").isNotNull())
)

dup_count = flagged_duplicates.count()
print(f"Flagged potential duplicate/near-duplicate awards: {dup_count}")
print("\nSample flagged records:")
flagged_duplicates.select(
    "recipient_name_clean", "award_id", "award_amount", "start_date_parsed",
    "prev_amount", "prev_date", "days_since_prev", F.round("pct_amount_diff", 2).alias("pct_amount_diff")
).orderBy(F.desc("award_amount")).show(15, truncate=False)

flagged_duplicates.write.format("delta").mode("overwrite").saveAsTable("gold_duplicate_awards")
print("\nGold table 'gold_duplicate_awards' written.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cell 3 — Test: Timing Anomalies (Fiscal Year-End Spikes)

# COMMAND ----------

from pyspark.sql import functions as F

silver_awards = spark.table("silver_usaspending_awards")

# Federal fiscal year ends Sept 30 — flag awards in the last 30 days of FY (Sept 1-30)
df = silver_awards.withColumn("award_month", F.month("start_date_parsed")) \
                   .withColumn("award_day", F.dayofmonth("start_date_parsed")) \
                   .withColumn(
                       "is_fy_end_period",
                       (F.col("award_month") == 9)
                   )

monthly_summary = (
    df.groupBy(F.month("start_date_parsed").alias("month"))
      .agg(
          F.sum("award_amount").alias("total_spend"),
          F.count("*").alias("award_count")
      )
      .orderBy("month")
)

print("Spend by calendar month (all years combined):")
monthly_summary.show(12)

avg_monthly_spend = monthly_summary.agg(F.avg("total_spend")).collect()[0][0]
sept_spend = monthly_summary.filter(F.col("month") == 9).collect()[0]["total_spend"]
spike_ratio = round(sept_spend / avg_monthly_spend, 2)

print(f"\nAverage monthly spend (all months): ${avg_monthly_spend:,.2f}")
print(f"September (fiscal year-end) spend: ${sept_spend:,.2f}")
print(f"September spend is {spike_ratio}x the average month")

gold_timing_risk = df.filter(F.col("is_fy_end_period") == True)
gold_timing_risk.write.format("delta").mode("overwrite").saveAsTable("gold_timing_risk")
print(f"\nGold table 'gold_timing_risk' written — {gold_timing_risk.count()} awards flagged as fiscal year-end activity.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Module 2 — the synthetic ERP journal entry _tests_

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cell 1 — Test: Benford's Law on Journal Entry Amounts

# COMMAND ----------

from pyspark.sql import functions as F

silver_je = spark.table("silver_fact_journal_entry")

# Benford's Law: in naturally-occurring financial data, the leading digit
# 1 appears ~30.1% of the time, 2 appears ~17.6%, down to 9 at ~4.6%.
# Significant deviation from this expected distribution is a classic red flag.

benford_expected = {
    1: 30.1, 2: 17.6, 3: 12.5, 4: 9.7, 5: 7.9,
    6: 6.7, 7: 5.8, 8: 5.1, 9: 4.6
}

df = silver_je.filter(F.col("amount") > 0).withColumn(
    "leading_digit",
    F.substring(F.col("amount").cast("string"), 1, 1).cast("int")
).filter(F.col("leading_digit").between(1, 9))

total_count = df.count()

actual_dist = (
    df.groupBy("leading_digit")
      .agg(F.count("*").alias("count"))
      .withColumn("actual_pct", F.round(F.col("count") / total_count * 100, 2))
      .orderBy("leading_digit")
)

print(f"Total journal entries analyzed: {total_count}\n")
print("Digit | Expected % | Actual % | Deviation")
results = actual_dist.collect()
for row in results:
    digit = row["leading_digit"]
    actual = row["actual_pct"]
    expected = benford_expected[digit]
    deviation = round(actual - expected, 2)
    flag = "  <-- FLAG" if abs(deviation) > 3.0 else ""
    print(f"  {digit}   |   {expected:5.1f}    |  {actual:5.2f}   |  {deviation:+.2f}{flag}")

actual_dist.write.format("delta").mode("overwrite").saveAsTable("gold_benford_analysis")
print("\nGold table 'gold_benford_analysis' written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2 — Test: Segregation of Duties (SoD) Conflicts
# MAGIC

# COMMAND ----------


from pyspark.sql import functions as F

silver_je = spark.table("silver_fact_journal_entry")
silver_user = spark.table("silver_dim_user")

sod_conflicts = silver_je.filter(F.col("created_by") == F.col("approved_by"))

conflict_count = sod_conflicts.count()
total_count = silver_je.count()
conflict_pct = round(conflict_count / total_count * 100, 2)

print(f"Total journal entries: {total_count}")
print(f"SoD conflicts (same user created AND approved): {conflict_count} ({conflict_pct}%)")

# Join to user table to see WHO is causing the most conflicts
conflict_by_user = (
    sod_conflicts
    .join(silver_user, sod_conflicts.created_by == silver_user.user_id)
    .groupBy("user_name", "role", "department")
    .agg(
        F.count("*").alias("conflict_count"),
        F.sum("amount").alias("total_amount_at_risk")
    )
    .orderBy(F.desc("conflict_count"))
)

print("\nTop users by SoD conflict count:")
conflict_by_user.show(10, truncate=False)

sod_conflicts.write.format("delta").mode("overwrite").saveAsTable("gold_sod_conflicts")
print("\nGold table 'gold_sod_conflicts' written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3 — Test: 3-Way Match Exceptions (PO vs GR vs Invoice)
# MAGIC

# COMMAND ----------


from pyspark.sql import functions as F

silver_po = spark.table("silver_fact_purchase_order")
silver_gr = spark.table("silver_fact_goods_receipt")
silver_inv = spark.table("silver_fact_invoice")

# Join PO -> GR -> Invoice on po_id
three_way = (
    silver_po
    .join(silver_gr, "po_id", "left")
    .join(silver_inv, "po_id", "left")
    .select(
        silver_po.po_id,
        silver_po.po_amount,
        silver_gr.gr_amount,
        silver_inv.invoice_amount,
        silver_inv.invoice_id
    )
)

# Case 1: PO exists but no Goods Receipt (control gap — goods never confirmed received)
missing_gr = three_way.filter(F.col("gr_amount").isNull())

# Case 2: PO exists but no Invoice (never billed — could be normal, or could be a gap)
missing_invoice = three_way.filter(F.col("invoice_amount").isNull())

# Case 3: the real exception — invoice amount doesn't match PO/GR amount
amount_mismatch = three_way.filter(
    F.col("invoice_amount").isNotNull() &
    F.col("po_amount").isNotNull() &
    (F.abs(F.col("invoice_amount") - F.col("po_amount")) > 0.01)
).withColumn(
    "variance_amount", F.round(F.col("invoice_amount") - F.col("po_amount"), 2)
).withColumn(
    "variance_pct", F.round((F.col("invoice_amount") - F.col("po_amount")) / F.col("po_amount") * 100, 2)
)

print(f"Total POs: {silver_po.count()}")
print(f"POs missing a Goods Receipt: {missing_gr.count()}")
print(f"POs missing an Invoice: {missing_invoice.count()}")
print(f"Invoice/PO amount mismatches (3-way match exceptions): {amount_mismatch.count()}")

print("\nTop amount mismatches by variance $:")
amount_mismatch.orderBy(F.desc("variance_amount")).select(
    "po_id", "invoice_id", "po_amount", "invoice_amount", "variance_amount", "variance_pct"
).show(15, truncate=False)

amount_mismatch.write.format("delta").mode("overwrite").saveAsTable("gold_3way_match_exceptions")
print("\nGold table 'gold_3way_match_exceptions' written.")

# COMMAND ----------

