# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title("🍹🍍 Pending Smoothie Orders 🍹🍍")
st.write("***Orders that need to be filled***")

session = get_active_session()

# 1️⃣ Get unfilled orders (KEEP ORDER_UID!)
my_dataframe = (
    session
    .table("smoothies.public.orders")
    .filter(col("ORDER_FILLED") == False)
)
# 2️⃣ Make it editable
if my_dataframe.count() > 0:
    editable_df = st.data_editor(my_dataframe)
# 3️⃣ Submit button
    Submit = st.button("Submit")
    if Submit:
# 4️⃣ Convert edited Streamlit df back to Snowpark df
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        try:
            # 5️⃣ Merge statement
            og_dataset.merge(edited_dataset,(og_dataset["ORDER_UID"] == edited_dataset["ORDER_UID"]),
            [when_matched().update({"ORDER_FILLED": edited_dataset["ORDER_FILLED"]})])
            st.success("Order Taken", icon="✅")
        except:
            st.write('Something went wrong')
else:
    st.success('There are no pending order right now', icon="✅")
