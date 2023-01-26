import secrets
from cumulusci.oauth.client import OAuth2Client, OAuth2ClientConfig
from cumulusci.tasks.salesforce import BaseSalesforceApiTask
from cumulusci.core.config.org_config import OrgConfig
from cumulusci.core.exceptions import (
    OrgAlreadyExists,
    OrgNotFound,
    SignupRequestError,
    SOQLQueryException,
)


class SignupOrgConfig(OrgConfig):
    def refresh_oauth_token(self, keychain, connected_app=None, is_sandbox=False):
        pass


class SignupRequest(BaseSalesforceApiTask):
    task_options = {
        "org_name": {
            "description": "The org name to use when connected the new org the cci keychain",
            "required": True,
        },
        "template_id": {
            "description": "The Trialforce template id (0TT...) to use in the SignupRequest",
            "required": True,
        },
        "company": {
            "description": "The name of the company to use in the SignupRequest",
        },
        "last_name": {
            "description": "The last name to use in the SignupRequest, defaults to CumulusCI",
        },
        "signup_email": {
            "description": "The email for the org admin.  Defaults to using email from github service",
        },
    }

    def _init_task(self):
        super()._init_task()
        self.connected_app = self.project_config.keychain.get_service("connected_app")
        self.auth_code = None
        self.return_values["instance_name"] = None
        self.return_values["instance_url"] = None
        self.return_values["org_id"] = None
        self.return_values["org_type"] = None
        self.return_values["signup_id"] = None
        self.return_values["username"] = None

    def _init_options(self, kwargs):
        super()._init_options(kwargs)
        try:
            org = self.project_config.keychain.get_org(self.options["org_name"])
            if org:
                raise OrgAlreadyExists(
                    f"Org {self.options['org_name']} already exists.  Use `cci org remove {self.options['org_name']}` to remove it first."
                )
        except OrgNotFound:
            pass

    def _run_task(self):
        email = self.options.get(
            "signup_email", self.project_config.keychain.get_service("github").email
        )
        username = email.replace(
            "@", f"@{self.options['org_name']}_{secrets.token_urlsafe(16)}."
        )
        subdomain = f"{self.options['org_name']}-{secrets.token_urlsafe(16)}".replace(
            "_", ""
        )
        self.return_values["instance_url"] = f"https://{subdomain}.my.salesforce.com"

        response = self.sf.SignupRequest.create(
            {
                "Company": self.options.get("company", "CumulusCI"),
                "Country": "US",
                "LastName": self.options.get("last_name", "CumulusCI"),
                "SignupEmail": email,
                "Username": username,
                "Subdomain": subdomain,
                "TemplateId": self.options["template_id"],
                "ConnectedAppCallbackUrl": self.connected_app.callback_url,
                "ConnectedAppConsumerKey": self.connected_app.client_id,
            }
        )
        self.return_values["signup_id"] = response["id"]
        # Wait for the signuprequest to complete
        self._poll()
        auth_result = self._exchange_auth_code()
        config = self.return_values.copy()
        config.update(auth_result)
        org_config = SignupOrgConfig(
            name=self.options["org_name"],
            config=config,
        )
        self.project_config.keychain.set_org(org_config, save=True)
        self.logger.info(
            f"Org {self.options['org_name']} successfully created and added to the keychain"
        )

    def _poll_action(self):
        soql = f"SELECT AuthCode, CreatedOrgId, CreatedOrgInstance, Edition, ErrorCode, Status, Username FROM SignupRequest WHERE Id='{self.return_values['signup_id']}'"
        res = self.sf.query(soql)
        if res["records"] == 0:
            raise SOQLQueryException(f"SOQL query found no records: {soql} ")
        signup = res["records"][0]
        status = signup["Status"]
        if status == "Error":
            raise SignupRequestError(
                f"SignupRequest {self.return_values['signup_id']} failed with error: {signup['ErrorCode']}"
            )
        elif status == "Success":
            self.auth_code = signup["AuthCode"]
            self.return_values["instance_name"] = signup["CreatedOrgInstance"]
            self.return_values["org_id"] = signup["CreatedOrgId"]
            self.return_values["org_type"] = signup["Edition"]
            self.return_values["username"] = signup["Username"]
            self.poll_complete = True
        else:
            self.logger.info(
                f"SignupRequest {self.return_values['signup_id']} Status: {status}"
            )

    def _exchange_auth_code(self):
        oauth_config = OAuth2ClientConfig(
            client_id=self.connected_app.client_id,
            client_secret=self.connected_app.client_secret,
            redirect_uri=self.connected_app.callback_url,
            auth_uri=f"{self.return_values['instance_url']}/services/oauth2/authorize",
            token_uri=f"{self.return_values['instance_url']}/services/oauth2/token",
            scope="web full refresh_token",
        )
        oauth = OAuth2Client(oauth_config)
        auth_result = oauth.auth_code_grant(self.auth_code).json()
        return auth_result
