import os
import random
import time
import argparse


def sample_tpc_h_subset_with_integrity(
    input_dir, output_dir, sample_percentage, random_seed=None
):
    """
    Samples CUSTOMER, ORDERS, and LINEITEM tables from TPC-H data while maintaining referential integrity.

    Args:
        input_dir (str): Path to the input directory containing original .tbl files.
        output_dir (str): Path to the output directory for sampled .tbl files.
        sample_percentage (float): Sampling percentage for the LINEITEM table (between 0.0 and 1.0).
                                   Other tables will be filtered based on LINEITEM's references.
        random_seed (int, optional): Seed for the random number generator to ensure reproducibility.
    """
    if random_seed is not None:
        random.seed(random_seed)
        print(f"Random seed set to: {random_seed}")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    # Store keys of orders and customers to be retained
    required_order_keys = set()
    required_customer_keys = set()

    # --- Step 1: Sample LINEITEM table and collect required ORDER_KEYs ---
    lineitem_input_path = os.path.join(input_dir, "lineitem.tbl")
    lineitem_output_path = os.path.join(output_dir, "lineitem.tbl")

    lines_processed_li = 0
    lines_sampled_li = 0
    start_time = time.time()

    print(f"--- Step 1/3: Sampling lineitem.tbl and collecting order keys ---")
    try:
        with open(lineitem_input_path, "r", encoding="utf-8") as infile_li, open(
            lineitem_output_path, "w", encoding="utf-8"
        ) as outfile_li:
            for line in infile_li:
                lines_processed_li += 1
                if random.random() < sample_percentage:
                    outfile_li.write(line)
                    lines_sampled_li += 1
                    # L_ORDERKEY is the first field (index 0) in lineitem.tbl
                    # TPC-H .tbl files are | delimited, with a trailing |
                    parts = line.strip().split("|")
                    if len(parts) > 0:
                        required_order_keys.add(int(parts[0]))
        print(
            f"  - Completed lineitem.tbl: Processed {lines_processed_li} lines, sampled {lines_sampled_li} lines."
        )
        print(
            f"  - Collected {len(required_order_keys)} unique order keys. (Time taken: {time.time() - start_time:.2f} seconds)\n"
        )
    except FileNotFoundError:
        print(
            f"Error: File not found {lineitem_input_path}. Please check your input directory setting."
        )
        return
    except Exception as e:
        print(f"An error occurred while processing lineitem.tbl: {e}")
        return

    # --- Step 2: Filter ORDERS table based on collected ORDER_KEYs and collect required CUST_KEYs ---
    orders_input_path = os.path.join(input_dir, "orders.tbl")
    orders_output_path = os.path.join(output_dir, "orders.tbl")

    lines_processed_o = 0
    lines_filtered_o = 0
    start_time = time.time()

    print(f"--- Step 2/3: Filtering orders.tbl and collecting customer keys ---")
    try:
        with open(orders_input_path, "r", encoding="utf-8") as infile_o, open(
            orders_output_path, "w", encoding="utf-8"
        ) as outfile_o:
            for line in infile_o:
                lines_processed_o += 1
                # O_ORDERKEY is the first field (index 0) in orders.tbl
                # O_CUSTKEY is the second field (index 1) in orders.tbl
                parts = line.strip().split("|")
                if len(parts) > 1:
                    order_key = int(parts[0])
                    if order_key in required_order_keys:
                        outfile_o.write(line)
                        lines_filtered_o += 1
                        required_customer_keys.add(int(parts[1]))  # O_CUSTKEY
        print(
            f"  - Completed orders.tbl: Processed {lines_processed_o} lines, filtered {lines_filtered_o} lines."
        )
        print(
            f"  - Collected {len(required_customer_keys)} unique customer keys. (Time taken: {time.time() - start_time:.2f} seconds)\n"
        )
    except FileNotFoundError:
        print(
            f"Error: File not found {orders_input_path}. Please check your input directory setting."
        )
        return
    except Exception as e:
        print(f"An error occurred while processing orders.tbl: {e}")
        return

    # --- Step 3: Filter CUSTOMER table based on collected CUST_KEYs ---
    customer_input_path = os.path.join(input_dir, "customer.tbl")
    customer_output_path = os.path.join(output_dir, "customer.tbl")

    lines_processed_c = 0
    lines_filtered_c = 0
    start_time = time.time()

    print(f"--- Step 3/3: Filtering customer.tbl ---")
    try:
        with open(customer_input_path, "r", encoding="utf-8") as infile_c, open(
            customer_output_path, "w", encoding="utf-8"
        ) as outfile_c:
            for line in infile_c:
                lines_processed_c += 1
                # C_CUSTKEY is the first field (index 0) in customer.tbl
                parts = line.strip().split("|")
                if len(parts) > 0:
                    cust_key = int(parts[0])
                    if cust_key in required_customer_keys:
                        outfile_c.write(line)
                        lines_filtered_c += 1
        print(
            f"  - Completed customer.tbl: Processed {lines_processed_c} lines, filtered {lines_filtered_c} lines."
        )
        print(f"  - (Time taken: {time.time() - start_time:.2f} seconds)\n")
    except FileNotFoundError:
        print(
            f"Error: File not found {customer_input_path}. Please check your input directory setting."
        )
        return
    except Exception as e:
        print(f"An error occurred while processing customer.tbl: {e}")
        return

    print("----------------------------------------------------------------")
    print("Sampling complete.")
    print(
        f"lineitem.tbl original rows: {lines_processed_li}, sampled rows: {lines_sampled_li}"
    )
    print(
        f"orders.tbl original rows: {lines_processed_o}, filtered rows: {lines_filtered_o}"
    )
    print(
        f"customer.tbl original rows: {lines_processed_c}, filtered rows: {lines_filtered_c}"
    )
    print(
        "\nImportant Note: This script only guarantees referential integrity between CUSTOMER, ORDERS, and LINEITEM tables."
    )
    print(
        "The TPC-H dataset has complex relationships with other tables (e.g., PART, SUPPLIER), which are not handled by this script."
    )
    print(
        "If you need a fully TPC-H compliant smaller dataset, please use the DBGEN tool."
    )
    print("----------------------------------------------------------------")


# --- Execute Sampling ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sample TPC-H CUSTOMER, ORDERS, and LINEITEM tables while maintaining referential integrity."
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        required=True,
        help="Directory containing original TPC-H .tbl files (e.g., 'data' or 'tpc_h_data/sf100').",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Directory to store sampled .tbl files.",
    )
    parser.add_argument(
        "--sample_percentage",
        type=float,
        required=True,
        help="Sampling percentage for the LINEITEM table (e.g., 0.01 for 1%%). "
        "Other tables will be filtered based on LINEITEM's references.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility. Default is 42.",
    )

    args = parser.parse_args()

    sample_tpc_h_subset_with_integrity(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        sample_percentage=args.sample_percentage,
        random_seed=args.seed,
    )
