import pandas as pd


def resp2frame(resp):
    """
    Place reformatting outside of primary execute method, easier to improve
    this way.

    TODO:
    This is VERY incomplete and pure kludge at this point!
    """
    out = pd.DataFrame()

    for rpt in resp.get('reports', []):
        # column names
        col_hdrs = rpt.get('columnHeader', {})
        cols = col_hdrs['dimensions']

        if 'metricHeader' in col_hdrs.keys():
            metrics = col_hdrs.get('metricHeader', {}).get('metricHeaderEntries', [])

            for m in metrics:
                # no effort made here to retain the dtype of the column
                cols = cols + [m.get('name', '')]

        df = pd.DataFrame(columns=cols)

        rows = rpt.get('data', {}).get('rows')
        if not rows:
            return df
        for row in rows:
            d = row.get('dimensions', [])

            if 'metrics' in row.keys():
                metrics = row.get('metrics', [])
                for m in metrics:
                    # TODO:
                    # this will likely not work in general
                    d = d + m.get('values', '')

            drow = {}
            for i, c in enumerate(cols):
                drow.update({c: d[i]})

            df = pd.concat((df, pd.DataFrame(drow, index=[0])), ignore_index=True, sort=False)

        out = pd.concat((out, df), ignore_index=True, sort=False)

    # get rid of the annoying 'ga:' bits on each column name
    for col in out.columns:
        out.rename(columns={col: col[3:]}, inplace=True)

    return out
