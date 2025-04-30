
# --- Configuration ---
EXISTING_ROLE_NAME = "..."  # Replace with the actual role name
NEW_USER_NAME = "..."        # Replace with the desired new user name
# Optional: Specify AWS region and profile if not using default
# AWS_REGION = "us-east-1"
# AWS_PROFILE = "default"
# iam_client = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION).client('iam')

import boto3
import json
import time
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# Define the maximum size for a managed policy document (in bytes)
MANAGED_POLICY_MAX_BYTES = 6144
# Define the maximum length for an IAM policy name
MAX_POLICY_NAME_LENGTH = 128
# --- End Configuration ---

def generate_single_policy_name(user_name: str, original_policy_name: str) -> str:
    """Generates a name for a non-split managed policy, ensuring it fits the length limit."""
    base_name = f"{user_name}-{original_policy_name}"
    return base_name[:MAX_POLICY_NAME_LENGTH]

def generate_split_policy_name(user_name: str, original_policy_name: str, part_number: int) -> str:
    """Generates a name for a split managed policy part, ensuring it fits the length limit."""
    base_prefix = f"{user_name}-{original_policy_name}"
    suffix = f"-Part{part_number}"
    # Calculate max length for the base_prefix part
    max_base_len = MAX_POLICY_NAME_LENGTH - len(suffix)
    truncated_base = base_prefix[:max_base_len]
    return f"{truncated_base}{suffix}"

def get_existing_managed_policy_arn(iam_client, policy_name: str) -> str | None:
    """Checks if a customer managed policy with the given name exists and returns its ARN."""
    try:
        # We need the account ID to construct the potential ARN for get_policy
        # Cache the STS client and account ID if running this many times
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity()['Account']
        policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"

        # Try getting the policy directly by ARN
        iam_client.get_policy(PolicyArn=policy_arn)
        print(f"    Found existing managed policy '{policy_name}' with ARN: {policy_arn}")
        return policy_arn
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            # Policy does not exist
            return None
        elif e.response['Error']['Code'] == 'InvalidInput':
             print(f"    Warning: Could not construct valid ARN for policy name '{policy_name}'. Assuming it doesn't exist.")
             return None
        else:
            print(f"    Warning: Error checking for existing policy '{policy_name}': {e}. Proceeding with creation attempt.")
            return None
    except (NoCredentialsError, PartialCredentialsError):
         print("    Warning: AWS credentials error while checking for existing policy. Proceeding with creation attempt.")
         return None

def calculate_required_parts(policy_version: str, statements: list, max_bytes: int) -> tuple[int, int]:
    """
    Calculates the number of parts a policy will be split into and the number of oversized statements.

    Returns:
        tuple: (number_of_parts, number_of_oversized_statements)
    """
    if not statements:
        return 0, 0

    part_count = 0
    oversized_statement_count = 0
    current_chunk_statements = []
    statements_processed_count = 0

    for statement in statements:
        statements_processed_count += 1
        temp_chunk_statements = current_chunk_statements + [statement]
        temp_policy_doc = {'Version': policy_version, 'Statement': temp_chunk_statements}
        temp_policy_string = json.dumps(temp_policy_doc)
        temp_policy_size_bytes = len(temp_policy_string.encode('utf-8'))

        if temp_policy_size_bytes <= max_bytes:
            current_chunk_statements.append(statement)
        else:
            # Statement makes the current chunk too large. Finalize the PREVIOUS chunk.
            if current_chunk_statements:
                part_count += 1 # Count the completed chunk
                # Start new chunk with the current statement
                current_chunk_statements = [statement]
                # Check if this single statement *itself* is too large
                single_statement_doc = {'Version': policy_version, 'Statement': current_chunk_statements}
                single_statement_string = json.dumps(single_statement_doc)
                single_statement_bytes = len(single_statement_string.encode('utf-8'))
                if single_statement_bytes > max_bytes:
                     oversized_statement_count += 1
                     current_chunk_statements = [] # Discard this statement
            else:
                # The very first statement processed was already too large
                oversized_statement_count += 1
                current_chunk_statements = [] # Ensure it's empty

    # Count the final remaining chunk if it has content
    if current_chunk_statements:
        part_count += 1

    return part_count, oversized_statement_count


def copy_inline_to_managed_policies(role_name: str, user_name: str):
    """
    Creates/updates an IAM user, retrieves inline policies from an existing IAM role,
    creates new Customer Managed Policies based on them (splitting if necessary),
    and attaches these managed policies to the user. Policies that are not split
    do not get the '-Part1' suffix.

    Args:
        role_name: The name of the existing IAM role.
        user_name: The name of the new IAM user to create or update.
    """
    managed_policies_parts_created = 0
    managed_policies_parts_attached = 0
    policies_processed = 0
    total_statements_skipped_oversize = 0
    policies_skipped_errors = 0 # Count policies skipped due to processing errors

    try:
        # Initialize Boto3 IAM client
        iam_client = boto3.client('iam')

        # --- 1. Create the new IAM user (or confirm existence) ---
        try:
            print(f"Ensuring IAM user exists: {user_name}...")
            iam_client.create_user(UserName=user_name)
            print(f"User {user_name} created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"User {user_name} already exists. Proceeding to manage policies.")
            else:
                print(f"Error creating/checking user {user_name}: {e}")
                return # Stop execution

        # --- 2. List inline policies of the existing role ---
        print(f"\nListing inline policies for role: {role_name}...")
        role_policy_names = []
        paginator = iam_client.get_paginator('list_role_policies')
        try:
            for page in paginator.paginate(RoleName=role_name):
                role_policy_names.extend(page.get('PolicyNames', []))
        except ClientError as e:
             print(f"Error listing policies for role {role_name}: {e}")
             if e.response['Error']['Code'] == 'NoSuchEntity':
                 print(f"Role {role_name} not found.")
             return # Stop if role doesn't exist or listing failed

        if not role_policy_names:
            print(f"No inline policies found for role {role_name}.")
            return

        print(f"Found {len(role_policy_names)} inline policies: {', '.join(role_policy_names)}")

        # --- 3. Process each role inline policy ---
        print("\nProcessing policies...")
        for original_policy_name in role_policy_names:
            policies_processed += 1
            print(f"\nProcessing original policy: {original_policy_name}...")
            try:
                # Get the policy document from the role
                response = iam_client.get_role_policy(
                    RoleName=role_name,
                    PolicyName=original_policy_name
                )
                policy_document = response['PolicyDocument'] # This is a dict
                original_statements = policy_document.get('Statement', [])
                policy_version = policy_document.get('Version', '2012-10-17')

                if not isinstance(original_statements, list):
                    print("  [!] ERROR: Policy 'Statement' field is not a list. Skipping this policy.")
                    policies_skipped_errors += 1
                    continue

                if not original_statements:
                     print("  [!] WARNING: Policy has an empty 'Statement' list. Skipping.")
                     continue # Nothing to create/attach

                # --- Pre-calculate the number of parts required ---
                total_parts, num_oversized_statements = calculate_required_parts(
                    policy_version, original_statements, MANAGED_POLICY_MAX_BYTES
                )
                total_statements_skipped_oversize += num_oversized_statements
                if num_oversized_statements > 0:
                     print(f"  [!] WARNING: {num_oversized_statements} statement(s) in this policy exceed the size limit and will be skipped.")

                if total_parts == 0:
                    print("  [!] Skipping policy: No valid parts could be generated (likely all statements were oversized).")
                    policies_skipped_errors += 1
                    continue
                else:
                    print(f"  Policy will be processed into {total_parts} part(s).")


                # --- Process statements into chunks and create/attach policies ---
                current_chunk_statements = []
                part_number = 1
                statements_processed_count = 0
                policy_fully_processed = True # Flag to track if all parts were handled

                for statement in original_statements:
                    statements_processed_count += 1
                    # Check if single statement is too large (pre-calculated, but double check here)
                    single_statement_doc_check = {'Version': policy_version, 'Statement': [statement]}
                    if len(json.dumps(single_statement_doc_check).encode('utf-8')) > MANAGED_POLICY_MAX_BYTES:
                         # This statement was identified as oversized in pre-calculation, skip it
                         continue

                    # Create temporary chunk to check size against ABSOLUTE limit
                    temp_chunk_statements = current_chunk_statements + [statement]
                    temp_policy_doc = {'Version': policy_version, 'Statement': temp_chunk_statements}
                    temp_policy_string = json.dumps(temp_policy_doc)
                    temp_policy_size_bytes = len(temp_policy_string.encode('utf-8'))

                    # Check against the ABSOLUTE limit for splitting decision
                    if temp_policy_size_bytes <= MANAGED_POLICY_MAX_BYTES:
                        # Statement fits in the current chunk (based on absolute byte size)
                        current_chunk_statements.append(statement)
                    else:
                        # Statement makes the current chunk too large. Finalize the PREVIOUS chunk.
                        if current_chunk_statements: # Ensure the previous chunk wasn't empty
                            # --- Process the completed chunk ---
                            chunk_policy_doc = {'Version': policy_version, 'Statement': current_chunk_statements}
                            chunk_policy_string = json.dumps(chunk_policy_doc)
                            chunk_policy_size_bytes = len(chunk_policy_string.encode('utf-8'))

                            # Determine policy name based on total parts
                            if total_parts == 1:
                                policy_name = generate_single_policy_name(user_name, original_policy_name)
                                print(f"  Single Part ready ({chunk_policy_size_bytes} bytes). Target name: '{policy_name}'")
                            else:
                                policy_name = generate_split_policy_name(user_name, original_policy_name, part_number)
                                print(f"  Chunk Part {part_number} ready ({chunk_policy_size_bytes} bytes). Target name: '{policy_name}'")


                            policy_arn_to_attach = get_existing_managed_policy_arn(iam_client, policy_name)

                            # Create if doesn't exist
                            if policy_arn_to_attach is None:
                                try:
                                    print(f"    Creating managed policy '{policy_name}'...")
                                    create_response = iam_client.create_policy(
                                        PolicyName=policy_name,
                                        PolicyDocument=chunk_policy_string,
                                        Description=(f"Policy from role {role_name}/{original_policy_name} for user {user_name}"
                                                     if total_parts == 1 else
                                                     f"Part {part_number} of policy from role {role_name}/{original_policy_name} for user {user_name}")
                                    )
                                    policy_arn_to_attach = create_response['Policy']['Arn']
                                    print(f"    Successfully created managed policy part with ARN: {policy_arn_to_attach}")
                                    managed_policies_parts_created += 1
                                    time.sleep(2) # Pause for consistency
                                except ClientError as create_error:
                                    if create_error.response['Error']['Code'] == 'EntityAlreadyExists':
                                        print(f"    [!] Warning: Managed policy part '{policy_name}' already exists (detected during creation). Attempting to find and attach.")
                                        policy_arn_to_attach = get_existing_managed_policy_arn(iam_client, policy_name)
                                        if policy_arn_to_attach is None:
                                            print(f"    [!] ERROR: Failed to get ARN for existing policy part '{policy_name}'. Skipping attachment for this part.")
                                            policy_fully_processed = False
                                            break # Exit statement loop for this original policy
                                    else:
                                        print(f"    [!] ERROR creating managed policy part '{policy_name}': {create_error}")
                                        policy_fully_processed = False
                                        break # Exit statement loop

                            # Attach the policy part
                            if policy_arn_to_attach:
                                try:
                                    print(f"    Attaching policy ARN '{policy_arn_to_attach}' to user '{user_name}'...")
                                    iam_client.attach_user_policy(
                                        UserName=user_name,
                                        PolicyArn=policy_arn_to_attach
                                    )
                                    print(f"    Successfully attached policy part.")
                                    managed_policies_parts_attached += 1
                                except ClientError as attach_error:
                                    print(f"    [!] ERROR attaching policy part ARN '{policy_arn_to_attach}': {attach_error}.")
                                    policy_fully_processed = False

                            part_number += 1 # Increment part number only after successful processing of a chunk
                            # --- End Process the completed chunk ---

                            # Start new chunk with the current statement that didn't fit
                            current_chunk_statements = [statement]
                            # Single oversized statement check already done via pre-calculation check

                        # else: # No previous chunk to process (first statement was too large)
                            # This case is handled by the pre-calculation skipping the statement

                # --- After loop: Process the final remaining chunk ---
                if current_chunk_statements and policy_fully_processed: # Only process if loop wasn't broken and chunk has content
                    # --- Process the final chunk ---
                    final_chunk_doc = {'Version': policy_version, 'Statement': current_chunk_statements}
                    final_chunk_string = json.dumps(final_chunk_doc)
                    final_chunk_bytes = len(final_chunk_string.encode('utf-8'))

                    # Determine policy name based on total parts
                    if total_parts == 1:
                         final_policy_name = generate_single_policy_name(user_name, original_policy_name)
                         print(f"  Single Part ready ({final_chunk_bytes} bytes). Target name: '{final_policy_name}'")
                    else:
                         final_policy_name = generate_split_policy_name(user_name, original_policy_name, part_number)
                         print(f"  Final Part {part_number} ready ({final_chunk_bytes} bytes). Target name: '{final_policy_name}'")


                    final_policy_arn_to_attach = get_existing_managed_policy_arn(iam_client, final_policy_name)

                    # Create if doesn't exist
                    if final_policy_arn_to_attach is None:
                        try:
                            print(f"    Creating final managed policy part '{final_policy_name}'...")
                            create_response = iam_client.create_policy(
                                PolicyName=final_policy_name,
                                PolicyDocument=final_chunk_string,
                                Description=(f"Policy from role {role_name}/{original_policy_name} for user {user_name}"
                                             if total_parts == 1 else
                                             f"Part {part_number} of policy from role {role_name}/{original_policy_name} for user {user_name}")
                            )
                            final_policy_arn_to_attach = create_response['Policy']['Arn']
                            print(f"    Successfully created final managed policy part with ARN: {final_policy_arn_to_attach}")
                            managed_policies_parts_created += 1
                            time.sleep(2) # Pause for consistency
                        except ClientError as create_error:
                            if create_error.response['Error']['Code'] == 'EntityAlreadyExists':
                                print(f"    [!] Warning: Final managed policy part '{final_policy_name}' already exists (detected during creation). Attempting to find and attach.")
                                final_policy_arn_to_attach = get_existing_managed_policy_arn(iam_client, final_policy_name)
                                if final_policy_arn_to_attach is None:
                                     print(f"    [!] ERROR: Failed to get ARN for existing final policy part '{final_policy_name}'. Skipping attachment.")
                                     policy_fully_processed = False
                            else:
                                print(f"    [!] ERROR creating final managed policy part '{final_policy_name}': {create_error}")
                                policy_fully_processed = False

                    # Attach the final policy part
                    if final_policy_arn_to_attach:
                        try:
                            print(f"    Attaching final policy part ARN '{final_policy_arn_to_attach}' to user '{user_name}'...")
                            iam_client.attach_user_policy(
                                UserName=user_name,
                                PolicyArn=final_policy_arn_to_attach
                            )
                            print(f"    Successfully attached final policy part.")
                            managed_policies_parts_attached += 1
                        except ClientError as attach_error:
                            print(f"    [!] ERROR attaching final policy part ARN '{final_policy_arn_to_attach}': {attach_error}.")
                            policy_fully_processed = False
                    # --- End Process the final chunk ---

                # Increment error count if policy wasn't fully processed
                if not policy_fully_processed:
                    policies_skipped_errors += 1


            except ClientError as e:
                print(f"  [!] ERROR processing original policy {original_policy_name}: {e}")
                policies_skipped_errors += 1 # Count the whole policy as skipped if getting it failed


        print("\n--- Script Summary ---")
        print(f"Total Role Inline Policies Found: {len(role_policy_names)}")
        print(f"Original Policies Processed: {policies_processed}")
        print(f"Managed Policy Parts Created: {managed_policies_parts_created}")
        print(f"Managed Policy Parts Attached (or found existing and attached): {managed_policies_parts_attached}")
        print(f"Statements Skipped (due to single statement > {MANAGED_POLICY_MAX_BYTES} bytes): {total_statements_skipped_oversize}")
        print(f"Original Policies Skipped or Not Fully Processed (due to errors): {policies_skipped_errors}")
        print("----------------------")


    except (NoCredentialsError, PartialCredentialsError):
        print("AWS credentials not found. Configure your credentials.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # --- Run the function ---
    copy_inline_to_managed_policies(EXISTING_ROLE_NAME, NEW_USER_NAME)
