import glob
import pandas as pd


results = {}

for con in range(1, 5):
    for ind in range(3, 28, 3):
        temp = []
        for mon in range(1, 7):
            name = "con-mon-ind/{con}-{mon}-{ind}.pkl".format(mon=mon, con=con, ind=ind)
            result_dict = pd.read_pickle(name)
            summary = result_dict["summary"]
            temp.append(summary["annualized_returns"])
        results.update({ind: temp})
    print("-" * 50)
    print("{con} Principal Component".format(con=con))
    results_df = pd.DataFrame(results, index=range(1, 7))
    print(results_df)





















# beifen:        for name in glob.glob("con-mon-ind/{con}-{mon}-*.pkl".format(
#                            mon=mon, con=con
#                        )):

# print("-" * 50)
# print("Sort by sharpe")
# print(results_df.sort_values("sharpe", ascending=False)[:20])
