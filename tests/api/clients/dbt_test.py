"""
Tests for the dbt client.
"""

# pylint: disable=missing-class-docstring, invalid-name, line-too-long

import datetime
from enum import Enum

import pytest
from marshmallow import fields
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker

from preset_cli import __version__
from preset_cli.api.clients.dbt import DBTClient, PostelEnumField
from preset_cli.auth.main import Auth


def test_postel_enum_field() -> None:
    """
    Test ``PostelEnumField``.
    """

    class StrEnum(str, Enum):
        A = "A"

    class IntEnum(int, Enum):
        A = 1

    class RawEnum(Enum):
        A = "A"
        B = 1

    assert isinstance(PostelEnumField(StrEnum, allow_none=True), fields.String)
    assert isinstance(PostelEnumField(IntEnum, allow_none=False), fields.Integer)
    assert isinstance(PostelEnumField(RawEnum), fields.Raw)


def test_dbt_client_execute(mocker: MockerFixture) -> None:
    """
    Test the ``execute`` method.
    """
    GraphqlClient = mocker.patch("preset_cli.api.clients.dbt.GraphqlClient")
    auth = Auth()
    client = DBTClient(auth)
    query = """
        query ($jobId: Int!) {
            models(jobId: $jobId) {
                uniqueId
                name
                database
                schema
                description
                meta
            }
        }
    """
    client.execute(query, jobId=1)
    GraphqlClient().execute.assert_called_with(
        query=query,
        variables={"jobId": 1},
        headers={
            "User-Agent": "Preset CLI",
            "X-Client-Version": __version__,
        },
    )


def test_dbt_client_get_accounts(requests_mock: Mocker) -> None:
    """
    Test the ``get_accounts`` method.
    """
    requests_mock.get(
        "https://cloud.getdbt.com/api/v2/accounts/",
        json={
            "status": {
                "code": 200,
                "is_success": True,
                "user_message": "Success!",
                "developer_message": "",
            },
            "data": [
                {
                    "docs_job_id": None,
                    "freshness_job_id": None,
                    "lock_reason": "[SYSTEM] Trial was expired as of 2022-06-24.",
                    "unlock_if_subscription_renewed": True,
                    "read_only_seats": 50,
                    "id": 72449,
                    "name": "Preset (Partner)",
                    "state": 1,
                    "plan": "free",
                    "pending_cancel": False,
                    "run_slots": 5,
                    "developer_seats": 50,
                    "queue_limit": 50,
                    "pod_memory_request_mebibytes": 600,
                    "run_duration_limit_seconds": 86400,
                    "enterprise_authentication_method": None,
                    "enterprise_login_slug": None,
                    "enterprise_unique_identifier": None,
                    "billing_email_address": None,
                    "locked": False,
                    "develop_file_system": True,
                    "unlocked_at": "2022-07-20T19:03:31.337718+00:00",
                    "created_at": "2022-06-09T21:42:07.004082+00:00",
                    "updated_at": "2022-07-20T19:03:31.341319+00:00",
                    "starter_repo_url": None,
                    "sso_reauth": True,
                    "force_sso": True,
                    "git_auth_level": "team",
                    "identifier": "act_2AMECVyWdvvg34fJWwVMWGtuTwq",
                    "docs_job": None,
                    "freshness_job": None,
                    "enterprise_login_url": "https://cloud.getdbt.com/enterprise-login/None/",
                },
            ],
            "extra": {
                "filters": {"pk": 72449},
                "order_by": None,
                "pagination": {"count": 1, "total_count": 1},
            },
        },
    )
    auth = Auth()
    client = DBTClient(auth)
    assert client.get_accounts() == [
        {
            "run_slots": 5,
            "created_at": datetime.datetime(
                2022,
                6,
                9,
                21,
                42,
                7,
                4082,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "name": "Preset (Partner)",
            "state": 1,
            "updated_at": datetime.datetime(
                2022,
                7,
                20,
                19,
                3,
                31,
                341319,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "read_only_seats": 50,
            "id": 72449,
            "developer_seats": 50,
            "plan": "free",
            "pending_cancel": False,
            "enterprise_login_url": "https://cloud.getdbt.com/enterprise-login/None/",
            "enterprise_authentication_method": None,
            "starter_repo_url": None,
            "force_sso": True,
            "develop_file_system": True,
            "lock_reason": "[SYSTEM] Trial was expired as of 2022-06-24.",
            "identifier": "act_2AMECVyWdvvg34fJWwVMWGtuTwq",
            "billing_email_address": None,
            "docs_job": None,
            "freshness_job": None,
            "enterprise_login_slug": None,
            "freshness_job_id": None,
            "locked": False,
            "unlocked_at": "2022-07-20T19:03:31.337718+00:00",
            "docs_job_id": None,
            "unlock_if_subscription_renewed": True,
            "run_duration_limit_seconds": 86400,
            "sso_reauth": True,
            "git_auth_level": "team",
            "enterprise_unique_identifier": None,
            "queue_limit": 50,
            "pod_memory_request_mebibytes": 600,
        },
    ]


def test_dbt_client_get_projects(requests_mock: Mocker) -> None:
    """
    Test the ``get_projects`` method.
    """
    requests_mock.get(
        "https://cloud.getdbt.com/api/v2/accounts/72449/projects/",
        json={
            "status": {
                "code": 200,
                "is_success": True,
                "user_message": "Success!",
                "developer_message": "",
            },
            "data": [
                {
                    "name": "Analytics",
                    "account_id": 72449,
                    "repository_id": 85562,
                    "connection_id": 79280,
                    "id": 113498,
                    "created_at": "2022-06-09 21:42:07.072884+00:00",
                    "updated_at": "2022-07-25 21:53:45.321518+00:00",
                    "skipped_setup": False,
                    "state": 1,
                    "dbt_project_subdirectory": None,
                    "connection": {
                        "id": 79280,
                        "account_id": 72449,
                        "project_id": 113498,
                        "name": "Bigquery",
                        "type": "bigquery",
                        "created_by_id": 89392,
                        "created_by_service_token_id": None,
                        "details": {
                            "project_id": "dbt-tutorial-347100",
                            "timeout_seconds": 300,
                            "private_key_id": "39f4cf7ffd957f8eaaf41e1009401f9c3fb039d6",
                            "client_email": "dbt-user@dbt-tutorial-347100.iam.gserviceaccount.com",
                            "client_id": "106702535903205632050",
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dbt-user%40dbt-tutorial-347100.iam.gserviceaccount.com",
                            "priority": "interactive",
                            "retries": 1,
                            "scopes": [
                                "https://www.googleapis.com/auth/bigquery",
                                "https://www.googleapis.com/auth/cloud-platform",
                                "https://www.googleapis.com/auth/drive",
                            ],
                            "location": None,
                            "maximum_bytes_billed": None,
                            "execution_project": None,
                            "impersonate_service_account": None,
                            "job_retry_deadline_seconds": None,
                            "job_creation_timeout_seconds": None,
                            "is_configured_for_oauth": False,
                        },
                        "state": 1,
                        "created_at": "2022-07-25 21:52:26.669562+00:00",
                        "updated_at": "2022-07-25 21:52:26.669581+00:00",
                    },
                    "repository": {
                        "id": 85562,
                        "account_id": 72449,
                        "project_id": 113498,
                        "full_name": "shreesham/preset-dbt-demo",
                        "remote_url": "git://github.com/shreesham/preset-dbt-demo.git",
                        "remote_backend": "github",
                        "git_clone_strategy": "github_app",
                        "deploy_key_id": 84338,
                        "repository_credentials_id": None,
                        "github_installation_id": 27417600,
                        "pull_request_url_template": "https://github.com/shreesham/preset-dbt-demo/compare/{{destination}}...{{source}}",
                        "state": 1,
                        "created_at": "2022-07-25 21:53:44.205393+00:00",
                        "updated_at": "2022-07-25 21:53:44.205413+00:00",
                        "deploy_key": {
                            "id": 84338,
                            "account_id": 72449,
                            "state": 1,
                            "public_key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCsuzl8uH0z0395SmYnlNuyeOCUqF7UhDVrIpUe+Vsyjin35cfnPop19k9l7nr+AZma+p2lf4NW9K1ugErB7hr5VUsSEoNjFk292z6dIhQVSLXrMJr3Kc0PixgXuDMgHUPQTU3T8zWeUmHQsslK0jTiRT/zH5ZdLc36AW9U2X9KriNHAfDHpx8b6eAgx2Prsvn/rnVqyolGlk9QBAPFqPzWbw231ORvWozUd94gklxJpWzIOvhZqVluuPs32FLquPfdkkRDjoVoLR1TQtriBBjaKsx6TK/frwtiOvrmbRm9l2ufXmHlahgTusbPru99z1rEwGlrqOI+DVo5VqM0ksavSz4auTJmzoIDhVXPgF2snwdnpfDeXiW9N/GQ90KGHGienBXnMeDCu+XcQrn+gUDimJfnRsSPSgU27eG/zWl6dEerlprV60g5WAViJ/MF1hBHZW+dpGJAipobvhA9NOA53xOHXYdVNn9uk9092/lQeiIBowhr8PKYezKMIf8z8cPKqL0BtUpBbYR/UpnmeWx9CmFbx9GxITiIWx+qOxZAzrLHwHrTfu1Rb20dXcE3XHSgmRFyyitOHn2+JN+rdZBT3XNePM++vygImnHoDCkJ/q11xmJWOEshsKESz0JrxGIkvXOJ3/2/Gw2Iujs17dFtXJADBzkNyuqGj4mD1Z/8gQ==",
                        },
                        "github_repo": "shreesham/preset-dbt-demo",
                        "name": "preset-dbt-demo",
                        "git_provider_id": 33120,
                        "gitlab": None,
                        "git_provider": None,
                    },
                    "group_permissions": [],
                    "docs_job_id": None,
                    "freshness_job_id": None,
                    "docs_job": None,
                    "freshness_job": None,
                },
                {
                    "name": "jaffle_shop",
                    "account_id": 72449,
                    "repository_id": 85563,
                    "connection_id": 79281,
                    "id": 134905,
                    "created_at": "2022-07-25 21:56:03.146661+00:00",
                    "updated_at": "2022-08-01 23:49:03.645162+00:00",
                    "skipped_setup": False,
                    "state": 1,
                    "dbt_project_subdirectory": "jaffle_shop",
                    "connection": {
                        "id": 79281,
                        "account_id": 72449,
                        "project_id": 134905,
                        "name": "Bigquery",
                        "type": "bigquery",
                        "created_by_id": 89392,
                        "created_by_service_token_id": None,
                        "details": {
                            "project_id": "dbt-tutorial-347100",
                            "timeout_seconds": 300,
                            "private_key_id": "39f4cf7ffd957f8eaaf41e1009401f9c3fb039d6",
                            "client_email": "dbt-user@dbt-tutorial-347100.iam.gserviceaccount.com",
                            "client_id": "106702535903205632050",
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dbt-user%40dbt-tutorial-347100.iam.gserviceaccount.com",
                            "priority": "interactive",
                            "retries": 1,
                            "scopes": [
                                "https://www.googleapis.com/auth/bigquery",
                                "https://www.googleapis.com/auth/cloud-platform",
                                "https://www.googleapis.com/auth/drive",
                            ],
                            "location": None,
                            "maximum_bytes_billed": None,
                            "execution_project": None,
                            "impersonate_service_account": None,
                            "job_retry_deadline_seconds": None,
                            "job_creation_timeout_seconds": None,
                            "is_configured_for_oauth": False,
                        },
                        "state": 1,
                        "created_at": "2022-07-25 21:57:25.938478+00:00",
                        "updated_at": "2022-07-25 21:57:25.938494+00:00",
                    },
                    "repository": {
                        "id": 85563,
                        "account_id": 72449,
                        "project_id": 134905,
                        "full_name": "preset-io/dbt-integration-blog-post",
                        "remote_url": "git://github.com/preset-io/dbt-integration-blog-post.git",
                        "remote_backend": "github",
                        "git_clone_strategy": "github_app",
                        "deploy_key_id": 84339,
                        "repository_credentials_id": None,
                        "github_installation_id": 27693839,
                        "pull_request_url_template": "https://github.com/preset-io/dbt-integration-blog-post/compare/{{destination}}...{{source}}",
                        "state": 1,
                        "created_at": "2022-07-25 21:57:36.748283+00:00",
                        "updated_at": "2022-07-25 21:57:36.748299+00:00",
                        "deploy_key": {
                            "id": 84339,
                            "account_id": 72449,
                            "state": 1,
                            "public_key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCQ6NWTsb48rEVFc7/Ihsz1XQ6dFa0TQEntuDu6uOPUxBVadup83vhKNqI9xRuOnDDEm/YqhcUZqx6OFB0BFmxSeEoRwTkDkURfBKh1nQ1kSbq0xbKwoRXyvwyfqs1HeTEtSZuPcKelMOWK3R5evCE3ZcKvLQw6oHf32JhHThuaOw/AG5OtvSLlWSxlQ6b7hW64K2kdsf+KODnD9+6PhEMludcBtIwTcZQdhHbyz/UMo3fGas59+LsxhBzjVyF1jmi3XrKLHJ+5oTlvz+Jr2EI2Srvftcciy/DvyGvG/biqrG1KjrCYoqzwjCFhBy4jJBU421QTjBtYLABVJC9hEtlsSFCFMJMubUhKELo3HhX6vbn5E8TF+UCP7wc7JEACyICzxeROk2QAb0aV7gmL/Zrc6mVNou5Cb5mVuv5NIUBjpx7m69rsHRR+lb4H41gZCeeh7dQmV08rZk0L47n8Clcuc55v99o8H7ekk+I9dOhHpeKvbW90XMGtDnZZLfw+o2wG/9S4js1b142nvFP55TfAb6+wOa4W+ZidaH5E3Mp41uk2wgLE2CsRVjAP5vM3OFTU6iyDY/obOMERNEhThKUS83ixR67s5haoWLRy+q36SvLn1oYQYlJ0UZp3SxZUnUTV1m20L2oQ4jAhnr5j3zzsNWCZLW4SxgdOq7Mi16Bqow==",
                        },
                        "github_repo": "preset-io/dbt-integration-blog-post",
                        "name": "dbt-integration-blog-post",
                        "git_provider_id": 33120,
                        "gitlab": None,
                        "git_provider": None,
                    },
                    "group_permissions": [],
                    "docs_job_id": None,
                    "freshness_job_id": None,
                    "docs_job": None,
                    "freshness_job": None,
                },
            ],
            "extra": {
                "filters": {"account_id": 72449, "limit": 100, "offset": 0},
                "order_by": "id",
                "pagination": {"count": 2, "total_count": 2},
            },
        },
    )
    auth = Auth()
    client = DBTClient(auth)
    assert client.get_projects(72449) == [
        {
            "connection": {
                "details": {
                    "project_id": "dbt-tutorial-347100",
                    "timeout_seconds": 300,
                    "private_key_id": "39f4cf7ffd957f8eaaf41e1009401f9c3fb039d6",
                    "client_email": "dbt-user@dbt-tutorial-347100.iam.gserviceaccount.com",
                    "client_id": "106702535903205632050",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dbt-user%40dbt-tutorial-347100.iam.gserviceaccount.com",
                    "priority": "interactive",
                    "retries": 1,
                    "scopes": [
                        "https://www.googleapis.com/auth/bigquery",
                        "https://www.googleapis.com/auth/cloud-platform",
                        "https://www.googleapis.com/auth/drive",
                    ],
                    "location": None,
                    "maximum_bytes_billed": None,
                    "execution_project": None,
                    "impersonate_service_account": None,
                    "job_retry_deadline_seconds": None,
                    "job_creation_timeout_seconds": None,
                    "is_configured_for_oauth": False,
                },
                "type": "bigquery",
                "id": 79280,
                "account_id": 72449,
                "state": 1,
                "updated_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    52,
                    26,
                    669581,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "created_by_id": 89392,
                "project_id": 113498,
                "created_by_service_token_id": None,
                "created_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    52,
                    26,
                    669562,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "name": "Bigquery",
            },
            "docs_job": None,
            "freshness_job_id": None,
            "freshness_job": None,
            "id": 113498,
            "connection_id": 79280,
            "account_id": 72449,
            "repository": {
                "gitlab": None,
                "deploy_key_id": 84338,
                "account_id": 72449,
                "repository_credentials_id": None,
                "git_provider_id": 33120,
                "git_clone_strategy": "github_app",
                "deploy_key": {
                    "id": 84338,
                    "account_id": 72449,
                    "state": 1,
                    "public_key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCsuzl8uH0z0395SmYnlNuyeOCUqF7UhDVrIpUe+Vsyjin35cfnPop19k9l7nr+AZma+p2lf4NW9K1ugErB7hr5VUsSEoNjFk292z6dIhQVSLXrMJr3Kc0PixgXuDMgHUPQTU3T8zWeUmHQsslK0jTiRT/zH5ZdLc36AW9U2X9KriNHAfDHpx8b6eAgx2Prsvn/rnVqyolGlk9QBAPFqPzWbw231ORvWozUd94gklxJpWzIOvhZqVluuPs32FLquPfdkkRDjoVoLR1TQtriBBjaKsx6TK/frwtiOvrmbRm9l2ufXmHlahgTusbPru99z1rEwGlrqOI+DVo5VqM0ksavSz4auTJmzoIDhVXPgF2snwdnpfDeXiW9N/GQ90KGHGienBXnMeDCu+XcQrn+gUDimJfnRsSPSgU27eG/zWl6dEerlprV60g5WAViJ/MF1hBHZW+dpGJAipobvhA9NOA53xOHXYdVNn9uk9092/lQeiIBowhr8PKYezKMIf8z8cPKqL0BtUpBbYR/UpnmeWx9CmFbx9GxITiIWx+qOxZAzrLHwHrTfu1Rb20dXcE3XHSgmRFyyitOHn2+JN+rdZBT3XNePM++vygImnHoDCkJ/q11xmJWOEshsKESz0JrxGIkvXOJ3/2/Gw2Iujs17dFtXJADBzkNyuqGj4mD1Z/8gQ==",
                },
                "full_name": "shreesham/preset-dbt-demo",
                "updated_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    53,
                    44,
                    205413,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "github_repo": "shreesham/preset-dbt-demo",
                "pull_request_url_template": "https://github.com/shreesham/preset-dbt-demo/compare/{{destination}}...{{source}}",
                "created_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    53,
                    44,
                    205393,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "git_provider": None,
                "id": 85562,
                "remote_url": "git://github.com/shreesham/preset-dbt-demo.git",
                "state": 1,
                "github_installation_id": 27417600,
                "project_id": 113498,
                "remote_backend": "github",
                "name": "preset-dbt-demo",
            },
            "state": 1,
            "updated_at": datetime.datetime(
                2022,
                7,
                25,
                21,
                53,
                45,
                321518,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "docs_job_id": None,
            "repository_id": 85562,
            "created_at": datetime.datetime(
                2022,
                6,
                9,
                21,
                42,
                7,
                72884,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "skipped_setup": False,
            "dbt_project_subdirectory": None,
            "group_permissions": [],
            "name": "Analytics",
        },
        {
            "connection": {
                "details": {
                    "project_id": "dbt-tutorial-347100",
                    "timeout_seconds": 300,
                    "private_key_id": "39f4cf7ffd957f8eaaf41e1009401f9c3fb039d6",
                    "client_email": "dbt-user@dbt-tutorial-347100.iam.gserviceaccount.com",
                    "client_id": "106702535903205632050",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dbt-user%40dbt-tutorial-347100.iam.gserviceaccount.com",
                    "priority": "interactive",
                    "retries": 1,
                    "scopes": [
                        "https://www.googleapis.com/auth/bigquery",
                        "https://www.googleapis.com/auth/cloud-platform",
                        "https://www.googleapis.com/auth/drive",
                    ],
                    "location": None,
                    "maximum_bytes_billed": None,
                    "execution_project": None,
                    "impersonate_service_account": None,
                    "job_retry_deadline_seconds": None,
                    "job_creation_timeout_seconds": None,
                    "is_configured_for_oauth": False,
                },
                "type": "bigquery",
                "id": 79281,
                "account_id": 72449,
                "state": 1,
                "updated_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    57,
                    25,
                    938494,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "created_by_id": 89392,
                "project_id": 134905,
                "created_by_service_token_id": None,
                "created_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    57,
                    25,
                    938478,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "name": "Bigquery",
            },
            "docs_job": None,
            "freshness_job_id": None,
            "freshness_job": None,
            "id": 134905,
            "connection_id": 79281,
            "account_id": 72449,
            "repository": {
                "gitlab": None,
                "deploy_key_id": 84339,
                "account_id": 72449,
                "repository_credentials_id": None,
                "git_provider_id": 33120,
                "git_clone_strategy": "github_app",
                "deploy_key": {
                    "id": 84339,
                    "account_id": 72449,
                    "state": 1,
                    "public_key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCQ6NWTsb48rEVFc7/Ihsz1XQ6dFa0TQEntuDu6uOPUxBVadup83vhKNqI9xRuOnDDEm/YqhcUZqx6OFB0BFmxSeEoRwTkDkURfBKh1nQ1kSbq0xbKwoRXyvwyfqs1HeTEtSZuPcKelMOWK3R5evCE3ZcKvLQw6oHf32JhHThuaOw/AG5OtvSLlWSxlQ6b7hW64K2kdsf+KODnD9+6PhEMludcBtIwTcZQdhHbyz/UMo3fGas59+LsxhBzjVyF1jmi3XrKLHJ+5oTlvz+Jr2EI2Srvftcciy/DvyGvG/biqrG1KjrCYoqzwjCFhBy4jJBU421QTjBtYLABVJC9hEtlsSFCFMJMubUhKELo3HhX6vbn5E8TF+UCP7wc7JEACyICzxeROk2QAb0aV7gmL/Zrc6mVNou5Cb5mVuv5NIUBjpx7m69rsHRR+lb4H41gZCeeh7dQmV08rZk0L47n8Clcuc55v99o8H7ekk+I9dOhHpeKvbW90XMGtDnZZLfw+o2wG/9S4js1b142nvFP55TfAb6+wOa4W+ZidaH5E3Mp41uk2wgLE2CsRVjAP5vM3OFTU6iyDY/obOMERNEhThKUS83ixR67s5haoWLRy+q36SvLn1oYQYlJ0UZp3SxZUnUTV1m20L2oQ4jAhnr5j3zzsNWCZLW4SxgdOq7Mi16Bqow==",
                },
                "full_name": "preset-io/dbt-integration-blog-post",
                "updated_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    57,
                    36,
                    748299,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "github_repo": "preset-io/dbt-integration-blog-post",
                "pull_request_url_template": "https://github.com/preset-io/dbt-integration-blog-post/compare/{{destination}}...{{source}}",
                "created_at": datetime.datetime(
                    2022,
                    7,
                    25,
                    21,
                    57,
                    36,
                    748283,
                    tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
                ),
                "git_provider": None,
                "id": 85563,
                "remote_url": "git://github.com/preset-io/dbt-integration-blog-post.git",
                "state": 1,
                "github_installation_id": 27693839,
                "project_id": 134905,
                "remote_backend": "github",
                "name": "dbt-integration-blog-post",
            },
            "state": 1,
            "updated_at": datetime.datetime(
                2022,
                8,
                1,
                23,
                49,
                3,
                645162,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "docs_job_id": None,
            "repository_id": 85563,
            "created_at": datetime.datetime(
                2022,
                7,
                25,
                21,
                56,
                3,
                146661,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "skipped_setup": False,
            "dbt_project_subdirectory": "jaffle_shop",
            "group_permissions": [],
            "name": "jaffle_shop",
        },
    ]


def test_dbt_client_get_jobs(requests_mock: Mocker) -> None:
    """
    Test the ``get_jobs`` method.
    """
    requests_mock.get(
        "https://cloud.getdbt.com/api/v2/accounts/72449/jobs/",
        json={
            "status": {
                "code": 200,
                "is_success": True,
                "user_message": "Success!",
                "developer_message": "",
            },
            "data": [
                {
                    "execution": {"timeout_seconds": 0},
                    "generate_docs": True,
                    "run_generate_sources": False,
                    "id": 108380,
                    "account_id": 72449,
                    "project_id": 134905,
                    "environment_id": 107605,
                    "name": "Test job",
                    "dbt_version": "1.0.0",
                    "created_at": "2022-07-25T22:00:11.943460+00:00",
                    "updated_at": "2022-07-26T22:36:23.862370+00:00",
                    "execute_steps": ["dbt run", "dbt test"],
                    "state": 1,
                    "deactivated": False,
                    "run_failure_count": 0,
                    "deferring_job_definition_id": None,
                    "lifecycle_webhooks": False,
                    "lifecycle_webhooks_url": None,
                    "triggers": {
                        "github_webhook": False,
                        "git_provider_webhook": False,
                        "custom_branch_only": False,
                        "schedule": True,
                    },
                    "settings": {"threads": 4, "target_name": "default"},
                    "schedule": {
                        "cron": "0 * * * *",
                        "date": {"type": "every_day"},
                        "time": {"type": "every_hour", "interval": 1},
                    },
                    "is_deferrable": False,
                    "generate_sources": False,
                    "cron_humanized": "Every hour",
                    "next_run": "2022-07-26T23:00:00+00:00",
                    "next_run_humanized": "2 weeks, 2 days",
                },
            ],
            "extra": {
                "filters": {"limit": 100, "offset": 0, "account_id": 72449},
                "order_by": "id",
                "pagination": {"count": 1, "total_count": 1},
            },
        },
    )
    auth = Auth()
    client = DBTClient(auth)
    assert client.get_jobs(72449) == [
        {
            "id": 108380,
            "deactivated": False,
            "triggers": {
                "custom_branch_only": False,
                "git_provider_webhook": False,
                "github_webhook": False,
                "schedule": True,
            },
            "next_run_humanized": "2 weeks, 2 days",
            "generate_sources": False,
            "execution": {"timeout_seconds": 0},
            "environment_id": 107605,
            "created_at": datetime.datetime(
                2022,
                7,
                25,
                22,
                0,
                11,
                943460,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "account_id": 72449,
            "state": 1,
            "deferring_job_definition_id": None,
            "generate_docs": True,
            "cron_humanized": "Every hour",
            "next_run": datetime.datetime(
                2022,
                7,
                26,
                23,
                0,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "run_failure_count": 0,
            "lifecycle_webhooks": False,
            "settings": {"threads": 4, "target_name": "default"},
            "execute_steps": ["dbt run", "dbt test"],
            "project_id": 134905,
            "is_deferrable": False,
            "schedule": {
                "cron": "0 * * * *",
                "time": {"interval": 1, "type": "every_hour"},
                "date": {"type": "every_day"},
            },
            "run_generate_sources": False,
            "dbt_version": "1.0.0",
            "updated_at": datetime.datetime(
                2022,
                7,
                26,
                22,
                36,
                23,
                862370,
                tzinfo=datetime.timezone(datetime.timedelta(0), "+0000"),
            ),
            "lifecycle_webhooks_url": None,
            "name": "Test job",
        },
    ]


def test_dbt_client_get_models(mocker: MockerFixture) -> None:
    """
    Test the ``get_models`` method.
    """
    GraphqlClient = mocker.patch("preset_cli.api.clients.dbt.GraphqlClient")
    GraphqlClient().execute.return_value = {
        "data": {
            "models": [
                {
                    "uniqueId": "model.jaffle_shop.customers",
                    "name": "customers",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "One record per customer",
                    "meta": {"superset": {"cache_timeout": 600}},
                },
                {
                    "uniqueId": "model.jaffle_shop.stg_customers",
                    "name": "stg_customers",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "This model cleans up customer data",
                    "meta": {},
                },
                {
                    "uniqueId": "model.jaffle_shop.stg_orders",
                    "name": "stg_orders",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "This model cleans up order data",
                    "meta": {},
                },
                {
                    "uniqueId": "model.metrics.dbt_metrics_default_calendar",
                    "name": "dbt_metrics_default_calendar",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "",
                    "meta": {},
                },
            ],
        },
    }
    auth = Auth()
    client = DBTClient(auth)
    assert client.get_models(108380) == [
        {
            "schema": "dbt_beto",
            "unique_id": "model.jaffle_shop.customers",
            "description": "One record per customer",
            "meta": {"superset": {"cache_timeout": 600}},
            "database": "dbt-tutorial-347100",
            "name": "customers",
        },
        {
            "schema": "dbt_beto",
            "unique_id": "model.jaffle_shop.stg_customers",
            "description": "This model cleans up customer data",
            "meta": {},
            "database": "dbt-tutorial-347100",
            "name": "stg_customers",
        },
        {
            "schema": "dbt_beto",
            "unique_id": "model.jaffle_shop.stg_orders",
            "description": "This model cleans up order data",
            "meta": {},
            "database": "dbt-tutorial-347100",
            "name": "stg_orders",
        },
        {
            "schema": "dbt_beto",
            "unique_id": "model.metrics.dbt_metrics_default_calendar",
            "description": "",
            "meta": {},
            "database": "dbt-tutorial-347100",
            "name": "dbt_metrics_default_calendar",
        },
    ]


def test_dbt_client_get_metrics(mocker: MockerFixture) -> None:
    """
    Test the ``get_metrics`` method.
    """
    GraphqlClient = mocker.patch("preset_cli.api.clients.dbt.GraphqlClient")
    GraphqlClient().execute.return_value = {
        "data": {
            "metrics": [
                {
                    "uniqueId": "metric.jaffle_shop.new_customers",
                    "name": "new_customers",
                    "label": "New Customers",
                    "type": "count",
                    "sql": "customer_id",
                    "filters": [
                        {"field": "number_of_orders", "operator": ">", "value": "0"},
                    ],
                    "dependsOn": ["model.jaffle_shop.customers"],
                    "description": "The number of paid customers using the product",
                    "meta": {},
                },
            ],
        },
    }
    auth = Auth()
    client = DBTClient(auth)
    assert client.get_metrics(108380) == [
        {
            "meta": {},
            "name": "new_customers",
            "type": "count",
            "label": "New Customers",
            "unique_id": "metric.jaffle_shop.new_customers",
            "description": "The number of paid customers using the product",
            "sql": "customer_id",
            "depends_on": ["model.jaffle_shop.customers"],
            "filters": [{"operator": ">", "value": "0", "field": "number_of_orders"}],
        },
    ]


def test_dbt_client_get_database_name(mocker: MockerFixture) -> None:
    """
    Test the ``get_database_name`` method.
    """
    GraphqlClient = mocker.patch("preset_cli.api.clients.dbt.GraphqlClient")
    GraphqlClient().execute.return_value = {
        "data": {
            "models": [
                {
                    "uniqueId": "model.jaffle_shop.customers",
                    "name": "customers",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "One record per customer",
                    "meta": {"superset": {"cache_timeout": 600}},
                },
                {
                    "uniqueId": "model.jaffle_shop.stg_customers",
                    "name": "stg_customers",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "This model cleans up customer data",
                    "meta": {},
                },
                {
                    "uniqueId": "model.jaffle_shop.stg_orders",
                    "name": "stg_orders",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "This model cleans up order data",
                    "meta": {},
                },
                {
                    "uniqueId": "model.metrics.dbt_metrics_default_calendar",
                    "name": "dbt_metrics_default_calendar",
                    "database": "dbt-tutorial-347100",
                    "schema": "dbt_beto",
                    "description": "",
                    "meta": {},
                },
            ],
        },
    }
    auth = Auth()
    client = DBTClient(auth)
    assert client.get_database_name(108380) == "dbt-tutorial-347100"


def test_dbt_client_get_database_name_no_models(mocker: MockerFixture) -> None:
    """
    Test the ``get_database_name`` method when there are no models.
    """
    GraphqlClient = mocker.patch("preset_cli.api.clients.dbt.GraphqlClient")
    GraphqlClient().execute.return_value = {"data": {"models": []}}
    auth = Auth()
    client = DBTClient(auth)

    with pytest.raises(Exception) as excinfo:
        client.get_database_name(108380)
    assert str(excinfo.value) == "No models found, can't determine database name"


def test_dbt_client_get_database_name_multiple(mocker: MockerFixture) -> None:
    """
    Test the ``get_database_name`` method when there are multiple databases.

    This shouldn't happen.
    """
    GraphqlClient = mocker.patch("preset_cli.api.clients.dbt.GraphqlClient")
    GraphqlClient().execute.return_value = {
        "data": {
            "models": [
                {
                    "uniqueId": "model.jaffle_shop.customers",
                    "name": "customers",
                    "database": "database_two",
                    "schema": "dbt_beto",
                    "description": "One record per customer",
                    "meta": {"superset": {"cache_timeout": 600}},
                },
                {
                    "uniqueId": "model.jaffle_shop.stg_customers",
                    "name": "stg_customers",
                    "database": "database_one",
                    "schema": "dbt_beto",
                    "description": "This model cleans up customer data",
                    "meta": {},
                },
            ],
        },
    }
    auth = Auth()
    client = DBTClient(auth)

    with pytest.raises(Exception) as excinfo:
        client.get_database_name(108380)
    assert str(excinfo.value) == "Multiple databases found"
