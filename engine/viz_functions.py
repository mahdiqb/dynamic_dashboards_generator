import plotly.express as px
import plotly.graph_objs as go
import re


def bar_chart(df, chart_type, title):
    """
    Creates a bar chart.
    """
    fig = px.bar(df, x=df.columns[0], y=df.columns[1])

    buttonlist = []
    for col in df.columns:
        buttonlist.append(
            dict(
                args=['y',[df[str(col)]] ],
                label=str(col),
                method='restyle'
            )
        )

    if title != '':
        chart_title = title
    else:
        chart_title = df.columns[0]+' - '+chart_type

    fig.update_layout(
            title=chart_title,
            yaxis_title="Values",
            xaxis_title=df.columns[0],
            # Add dropdown
            updatemenus=[
                go.layout.Updatemenu(
                    buttons=buttonlist,
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.1,
                    xanchor="left",
                    y=1.1,
                    yanchor="top"
                ),
            ],
            autosize=True
        )
    fig.show()
    

def line_chart(df, chart_type):
    """
    Creates a line chart.
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['article_date'].tolist(), y=df['count'].tolist(),
                        mode='lines+markers',
                        name='News count'))
    fig.update_layout(
    title="News aggregated by date",
    xaxis_title="Date",
    yaxis_title="Number of articles"
    )
    fig.show()

    
def pie_chart(df, chart_type, title):
    """
    Creates a pie chart.
    """
    fig = px.pie(df, names=df.columns[0], values=df.columns[1])

    buttonlist = []
    for col in df.columns:
        buttonlist.append(
            dict(
                args=['y',[df[str(col)]] ],
                label=str(col),
                method='restyle'
            )
        )

    fig.update_layout(
            title=title,
            # Add dropdown
            updatemenus=[
                go.layout.Updatemenu(
                    buttons=buttonlist,
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.1,
                    xanchor="left",
                    y=1.1,
                    yanchor="top"
                ),
            ],
            autosize=True
        )
    fig.show()
    

def show_graph(charts, df, index):
    """
    Calls the function corresponding to the chart to visualize.
    """
    title = ''
    if 'quandl' in list(charts)[index]:
        title = list(charts)[index].replace('aggregated_data2/quandl','')
    elif 'columns' in list(charts)[index]:
        title = re.sub(r'(?is)part.+', '', list(charts)[index].replace('aggregated_data2/columns',''))
    if list(charts.values())[index] == 'bar_chart':
        bar_chart(df, list(charts.values())[index], title.replace('.csv', ''))
    elif list(charts.values())[index] == 'pie_chart':
        pie_chart(df, list(charts.values())[index], title.replace('.csv', ''))
    elif list(charts.values())[index] == 'line_chart':
        line_chart(df, list(charts.values())[index])
    else:
        print("No chart available for the keyword "+df.columns[0])
