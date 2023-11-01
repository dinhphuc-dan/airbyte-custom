
SummarySales = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "Provider": {"type": ["null", "string"]},
    "Provider_Country": {"type": ["null", "string"]},
    "SKU": {"type": ["null", "string"]},
    "Developer": {"type": ["null", "string"]},
    "Title": {"type": ["null", "string"]},
    "Version": {"type": ["null", "string"]},
    "Product_Type_Identifier": {"type": ["null", "string"]},
    "Units": {"type": ["null", "integer"]},
    "Developer_Proceeds": {"type": ["null", "number"]},
    "Begin_Date": {"type": ["null", "string"]},
    "End_Date": {"type": ["null", "string"]},
    "Customer_Currency": {"type": ["null", "string"]},
    "Country_Code": {"type": ["null", "string"]},
    "Currency_of_Proceeds": {"type": ["null", "string"]},
    "Apple_Identifier": {"type": ["null", "integer"]},
    "Customer_Price": {"type": ["null", "number"]},
    "Promo_Code": {"type": ["null", "string"]},
    "Parent_Identifier": {"type": ["null", "string"]},
    "Subscription": {"type": ["null", "string"]},
    "Period": {"type": ["null", "string"]},
    "Category": {"type": ["null", "string"]},
    "CMB": {"type": ["null", "string"]},
    "Device": {"type": ["null", "string"]},
    "Supported_Platforms": {"type": ["null", "string"]},
    "Proceeds_Reason": {"type": ["null", "string"]},
    "Preserved_Pricing": {"type": ["null", "string"]},
    "Client": {"type": ["null", "string"]},
    "Order_Type": {"type": ["null", "string"]}
  }
}

SubscriptionEvent = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "Event_Date": {"type": ["null", "string"]},
    "Event": {"type": ["null", "string"]},
    "App_Name": {"type": ["null", "string"]},
    "App_Apple_ID": {"type": ["null", "string"]},
    "Subscription_Name": {"type": ["null", "string"]},
    "Subscription_Apple_ID": {"type": ["null", "integer"]},
    "Subscription_Group_ID": {"type": ["null", "integer"]},
    "Standard_Subscription_Duration": {"type": ["null", "string"]},
    "Subscription_Offer_Type": {"type": ["null", "string"]},
    "Subscription_Offer_Duration": {"type": ["null", "string"]},
    "Marketing_Opt-In": {"type": ["null", "string"]},
    "Marketing_Opt-In_Duration": {"type": ["null", "string"]},
    "Preserved_Pricing": {"type": ["null", "string"]},
    "Proceeds_Reason": {"type": ["null", "string"]},
    "Promotional_Offer_Name": {"type": ["null", "string"]},
    "Promotional_Offer_ID": {"type": ["null", "string"]},
    "Consecutive_Paid_Periods": {"type": ["null", "integer"]},
    "Original_Start_Date": {"type": ["null", "string"]},
    "Device": {"type": ["null", "string"]},
    "Client": {"type": ["null", "string"]},
    "State": {"type": ["null", "string"]},
    "Country": {"type": ["null", "string"]},
    "Previous_Subscription_Name": {"type": ["null", "string"]},
    "Previous_Subscription_Apple_ID": {"type": ["null", "integer"]},
    "Days_Before_Canceling": {"type": ["null", "integer"]},
    "Cancellation_Reason": {"type": ["null", "string"]},
    "Days_Canceled": {"type": ["null", "integer"]},
    "Quantity": {"type": ["null", "integer"]},
    "Paid_Service_Days_Recovered": {"type": ["null", "integer"]},
    }
}

def SummarySalesSchema() -> dict:
    return SummarySales

def SubscriptionEventSchema() -> dict:
    return SubscriptionEvent