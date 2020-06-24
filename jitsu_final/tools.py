import gspread


class SpreadSheetClient:
    # DEFAULT_CREDENTIALS_PATH = '~/.config/gspread/service_account.json'

    def __init__(self, creds_path=None):
        # if creds_path is None:
        #    creds_path = self.DEFAULT_CREDENTIALS_PATH
        if creds_path is not None:
            self.gc = gspread.service_account(filename=creds_path)
        else:
            self.gc = gspread.service_account()

    def open_spreadsheet(self, spreadsheet_name: str):
        self.sh = self.gc.open(spreadsheet_name)
        return self.sh
