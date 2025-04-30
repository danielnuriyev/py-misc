import boto3
from botocore.exceptions import ClientError, NoSuchEntityException

def delete_iam_user_and_policies(username_to_delete: str):
    """
    Deletes an IAM user and associated resources, including attached managed
    policies whose names start with the username prefix.

    Args:
        username_to_delete: The name of the IAM user to delete.

    Returns:
        None
    """
    iam_client = boto3.client('iam')
    print(f"--- Starting deletion process for user: {username_to_delete} ---")

    try:
        # 0. Check if user exists before proceeding
        try:
            iam_client.get_user(UserName=username_to_delete)
            print(f"User '{username_to_delete}' found. Proceeding with deletion.")
        except iam_client.exceptions.NoSuchEntityException:
            print(f"User '{username_to_delete}' not found. Exiting.")
            return
        except ClientError as e:
            print(f"Error checking user existence: {e}")
            return # Exit if we can't confirm user existence

        # 1. Delete Access Keys
        print("Step 1: Deleting access keys...")
        try:
            keys_paginator = iam_client.get_paginator('list_access_keys')
            for response in keys_paginator.paginate(UserName=username_to_delete):
                for key in response.get('AccessKeyMetadata', []):
                    access_key_id = key['AccessKeyId']
                    print(f"  Deleting access key: {access_key_id}")
                    iam_client.delete_access_key(
                        UserName=username_to_delete,
                        AccessKeyId=access_key_id
                    )
        except ClientError as e:
            print(f"  Could not list or delete access keys: {e}")

        # 2. Delete Signing Certificates
        print("Step 2: Deleting signing certificates...")
        try:
            certs_paginator = iam_client.get_paginator('list_signing_certificates')
            for response in certs_paginator.paginate(UserName=username_to_delete):
                for cert in response.get('Certificates', []):
                    cert_id = cert['CertificateId']
                    print(f"  Deleting signing certificate: {cert_id}")
                    iam_client.delete_signing_certificate(
                        UserName=username_to_delete,
                        CertificateId=cert_id
                    )
        except ClientError as e:
            print(f"  Could not list or delete signing certificates: {e}")

        # 3. Deactivate and Delete MFA Devices
        print("Step 3: Deactivating and deleting MFA devices...")
        try:
            mfa_paginator = iam_client.get_paginator('list_mfa_devices')
            for response in mfa_paginator.paginate(UserName=username_to_delete):
                for mfa_device in response.get('MFADevices', []):
                    serial_number = mfa_device['SerialNumber']
                    print(f"  Deactivating MFA device: {serial_number}")
                    iam_client.deactivate_mfa_device(
                        UserName=username_to_delete,
                        SerialNumber=serial_number
                    )
                    # Note: Virtual MFA devices are deleted automatically when deactivated.
                    # Hardware MFA devices remain in the account but are disassociated.
                    # If you need to explicitly delete virtual MFA devices (though usually not needed after deactivation):
                    # try:
                    #     print(f"  Deleting virtual MFA device: {serial_number}")
                    #     iam_client.delete_virtual_mfa_device(SerialNumber=serial_number)
                    # except ClientError as e:
                    #     # Ignore errors like NoSuchEntity if already deleted
                    #     if e.response['Error']['Code'] != 'NoSuchEntity':
                    #          print(f"    Could not delete virtual MFA device {serial_number}: {e}")

        except ClientError as e:
            print(f"  Could not list or deactivate MFA devices: {e}")

        # 4. Delete Login Profile (Console Password)
        print("Step 4: Deleting login profile...")
        try:
            iam_client.delete_login_profile(UserName=username_to_delete)
            print(f"  Login profile for '{username_to_delete}' deleted.")
        except iam_client.exceptions.NoSuchEntityException:
            print(f"  No login profile found for '{username_to_delete}'.")
        except ClientError as e:
            print(f"  Could not delete login profile: {e}")

        # 5. Detach and Potentially Delete Managed Policies
        print("Step 5: Detaching and potentially deleting managed policies...")
        policies_to_delete = []
        try:
            paginator = iam_client.get_paginator('list_attached_user_policies')
            for response in paginator.paginate(UserName=username_to_delete):
                for policy in response.get('AttachedPolicies', []):
                    policy_arn = policy['PolicyArn']
                    policy_name = policy['PolicyName']
                    print(f"  Detaching managed policy '{policy_name}' ({policy_arn})...")
                    try:
                        iam_client.detach_user_policy(
                            UserName=username_to_delete,
                            PolicyArn=policy_arn
                        )
                        print(f"    Successfully detached '{policy_name}'.")

                        # Check if it's a customer-managed policy and matches the prefix
                        is_customer_managed = not policy_arn.startswith('arn:aws:iam::aws:policy/')
                        matches_prefix = policy_name.startswith(username_to_delete)

                        if is_customer_managed and matches_prefix:
                            print(f"    Policy '{policy_name}' is customer-managed and matches prefix. Queuing for deletion.")
                            policies_to_delete.append(policy_arn)
                        elif not is_customer_managed:
                            print(f"    Policy '{policy_name}' is AWS managed. Skipping deletion.")
                        elif not matches_prefix:
                             print(f"    Policy '{policy_name}' does not match prefix '{username_to_delete}'. Skipping deletion.")

                    except ClientError as detach_error:
                        print(f"    Could not detach policy {policy_arn}: {detach_error}")

        except ClientError as list_error:
            print(f"  Could not list attached managed policies: {list_error}")

        # Attempt to delete the queued managed policies
        if policies_to_delete:
            print("  Attempting deletion of queued customer-managed policies...")
            for policy_arn in policies_to_delete:
                 policy_name = policy_arn.split('/')[-1] # Extract name for logging
                 print(f"    Deleting policy '{policy_name}' ({policy_arn})...")
                 try:
                     iam_client.delete_policy(PolicyArn=policy_arn)
                     print(f"      Successfully deleted policy '{policy_name}'.")
                 except iam_client.exceptions.DeleteConflictException:
                     print(f"      WARNING: Could not delete policy '{policy_name}' ({policy_arn}). It is likely attached to other entities (users, groups, or roles).")
                 except iam_client.exceptions.NoSuchEntityException:
                     print(f"      Policy '{policy_name}' ({policy_arn}) already deleted.") # Might happen in concurrent runs
                 except ClientError as delete_error:
                     print(f"      ERROR: Could not delete policy '{policy_name}' ({policy_arn}): {delete_error}")
        else:
             print("  No customer-managed policies with matching prefix found attached to this user.")


        # 6. Delete Inline Policies
        print("Step 6: Deleting inline policies...")
        try:
            inline_paginator = iam_client.get_paginator('list_user_policies')
            for response in inline_paginator.paginate(UserName=username_to_delete):
                for policy_name in response.get('PolicyNames', []):
                    print(f"  Deleting inline policy: {policy_name}")
                    iam_client.delete_user_policy(
                        UserName=username_to_delete,
                        PolicyName=policy_name
                    )
        except ClientError as e:
            print(f"  Could not list or delete inline policies: {e}")

        # 7. Remove User from Groups
        print("Step 7: Removing user from groups...")
        try:
            groups_paginator = iam_client.get_paginator('list_groups_for_user')
            for response in groups_paginator.paginate(UserName=username_to_delete):
                for group in response.get('Groups', []):
                    group_name = group['GroupName']
                    print(f"  Removing user from group: {group_name}")
                    iam_client.remove_user_from_group(
                        GroupName=group_name,
                        UserName=username_to_delete
                    )
        except ClientError as e:
            print(f"  Could not list or remove user from groups: {e}")

        # 8. Delete the User
        print(f"Step 8: Deleting user '{username_to_delete}'...")
        try:
            iam_client.delete_user(UserName=username_to_delete)
            print(f"--- Successfully deleted user: {username_to_delete} ---")
        except ClientError as e:
            print(f"  ERROR: Failed to delete user '{username_to_delete}': {e}")
            print("  This might be due to permissions issues or resources that were not cleaned up properly in previous steps.")

    except Exception as e:
        # Catch-all for unexpected errors during the process
        print(f"An unexpected error occurred during the deletion process for user '{username_to_delete}': {e}")

# --- Main execution ---
if __name__ == "__main__":
    # !!! IMPORTANT !!!
    # Replace 'your_username_here' with the actual IAM username you want to delete.
    # Double-check the username before running!
    target_username = "your_username_here" # <--- CHANGE THIS

    if target_username == "your_username_here":
        print("Please replace 'your_username_here' with the actual username in the script.")
    else:
        # Add a confirmation step for safety
        confirm = input(f"Are you sure you want to delete the IAM user '{target_username}' and potentially associated policies? This action is IRREVERSIBLE. (yes/no): ")
        if confirm.lower() == 'yes':
            delete_iam_user_and_policies(target_username)
        else:
            print("Deletion cancelled.")
