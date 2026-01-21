"""
Layout Components for Profile Page

Contains functions for creating the main layout structure of the profile page.
"""

from dash import dcc, html
import logging

logger = logging.getLogger(__name__)


def create_sidebar_content():
    """Create accordion-based sidebar content with collapsible sections."""
    
    # Enhanced filter options with better organization and descriptions
    filter_options = [
        {'label': '📊 All Filters', 'value': 'all'},
        {'label': '📈 Volume Filters', 'value': 'volume'},
        {'label': '📉 Price Filters', 'value': 'price'},
        {'label': '⏰ Time Filters', 'value': 'time'},
        {'label': '🎯 Custom Filters', 'value': 'custom'},
    ]
    
    # Product options
    product_options = [
                    {'label': 'ES (E-mini S&P 500)', 'value': 'ES'},
                    {'label': 'NQ (E-mini NASDAQ)', 'value': 'NQ'},
                    {'label': 'YM (E-mini Dow)', 'value': 'YM'},
                    {'label': 'RTY (E-mini Russell)', 'value': 'RTY'},
                    {'label': 'CL (Crude Oil)', 'value': 'CL'},
                    {'label': 'GC (Gold)', 'value': 'GC'},
                    {'label': 'SI (Silver)', 'value': 'SI'},
                    {'label': 'NG (Natural Gas)', 'value': 'NG'},
                    {'label': 'ZB (30-Year Bond)', 'value': 'ZB'},
                    {'label': 'ZN (10-Year Note)', 'value': 'ZN'},
                    {'label': 'ZF (5-Year Note)', 'value': 'ZF'},
                    {'label': 'ZT (2-Year Note)', 'value': 'ZT'},
        {'label': 'EUR (Euro)', 'value': 'EUR'},
        {'label': 'GBP (British Pound)', 'value': 'GBP'},
        {'label': 'JPY (Japanese Yen)', 'value': 'JPY'},
        {'label': 'AUD (Australian Dollar)', 'value': 'AUD'},
        {'label': 'CAD (Canadian Dollar)', 'value': 'CAD'},
        {'label': 'CHF (Swiss Franc)', 'value': 'CHF'},
        {'label': 'NZD (New Zealand Dollar)', 'value': 'NZD'},
        {'label': 'C (Corn)', 'value': 'C'},
        {'label': 'S (Soybeans)', 'value': 'S'},
        {'label': 'W (Wheat)', 'value': 'W'},
        {'label': 'TSLA (Tesla Stock)', 'value': 'TSLA'},
        {'label': 'BTC/USD (Bitcoin)', 'value': 'BTCUSD'},
    ]
    
    sidebar_content = [
        html.H2("📊 Almanac Futures", style={'marginBottom': '20px', 'fontSize': '20px'}),
        
        # Current Time Display
        html.Div([
            html.Label("🕐 Current Time:", style={'fontWeight': 'bold', 'marginBottom': '5px'}),
            html.Div(id='current-time-display', style={'fontSize': '12px', 'color': '#666'})
        ], style={'marginBottom': '20px', 'padding': '10px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px'}),
        
        # Product Selection
        html.Div([
            html.Label("📈 Product:", style={'fontWeight': 'bold', 'marginBottom': '5px'}),
            dcc.Dropdown(
                id='product-dropdown',
                options=product_options,
                value='ES',
                clearable=False,
                style={'fontSize': '12px'}
            )
        ], style={'marginBottom': '20px'}),
        
        # Date Range Selection
        html.Div([
            html.Label("📅 Date Range:", style={'fontWeight': 'bold', 'marginBottom': '10px'}),
            html.Div([
                html.Label("From:", style={'fontSize': '10px', 'marginBottom': '5px'}),
                dcc.DatePickerSingle(
                    id='filter-start-date',
                    date='2025-01-01',
                    style={'fontSize': '10px'}
                )
            ], style={'marginBottom': '10px'}),
            html.Div([
                html.Label("To:", style={'fontSize': '10px', 'marginBottom': '5px'}),
                dcc.DatePickerSingle(
                    id='filter-end-date',
                    date='2025-01-31',
                    style={'fontSize': '10px'}
                )
            ], style={'marginBottom': '10px'}),
            html.Button(
                "📅 Max Range",
                id='max-date-range-btn',
                n_clicks=0,
                style={
                    'width': '100%',
                    'padding': '8px',
                    'fontSize': '10px',
                    'backgroundColor': '#6c757d',
                    'color': 'white',
                    'border': 'none',
                    'borderRadius': '3px',
                    'cursor': 'pointer'
                }
            )
        ], style={'marginBottom': '20px', 'padding': '10px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px'}),
        
        # Month-of-Year Analysis Section
        html.Div([
            html.H4("📊 MONTH-OF-YEAR ANALYSIS", style={
                'color': '#2c3e50',
                'fontWeight': 'bold',
                'marginBottom': '15px',
                'textAlign': 'center',
                'fontSize': '14px',
                'textTransform': 'uppercase',
                'letterSpacing': '0.5px'
            }),
            html.Small("💡 Analyze yearly statistics and seasonal patterns", 
                      style={'color': '#666', 'fontSize': '10px', 'marginBottom': '15px', 'display': 'block'}),
            
            # Monthly Date Range
            html.Div([
                html.Label("📅 Monthly Range", style={'fontWeight': 'bold', 'marginBottom': '8px', 'fontSize': '12px'}),
                
                html.Div([
                    html.Div([
                        html.Label("From", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='monthly-from-month',
                            options=[
                                {'label': 'January', 'value': 1},
                                {'label': 'February', 'value': 2},
                                {'label': 'March', 'value': 3},
                                {'label': 'April', 'value': 4},
                                {'label': 'May', 'value': 5},
                                {'label': 'June', 'value': 6},
                                {'label': 'July', 'value': 7},
                                {'label': 'August', 'value': 8},
                                {'label': 'September', 'value': 9},
                                {'label': 'October', 'value': 10},
                                {'label': 'November', 'value': 11},
                                {'label': 'December', 'value': 12}
                            ],
                            value=1,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block', 'marginRight': '2%'}),
                    
                    html.Div([
                        html.Label("Year", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='monthly-from-year',
                            options=[{'label': str(year), 'value': year} for year in range(2010, 2026)],
                            value=2025,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block'})
                ], style={'marginBottom': '10px'}),
                
                html.Div([
                    html.Div([
                        html.Label("To", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='monthly-to-month',
                            options=[
                                {'label': 'January', 'value': 1},
                                {'label': 'February', 'value': 2},
                                {'label': 'March', 'value': 3},
                                {'label': 'April', 'value': 4},
                                {'label': 'May', 'value': 5},
                                {'label': 'June', 'value': 6},
                                {'label': 'July', 'value': 7},
                                {'label': 'August', 'value': 8},
                                {'label': 'September', 'value': 9},
                                {'label': 'October', 'value': 10},
                                {'label': 'November', 'value': 11},
                                {'label': 'December', 'value': 12}
                            ],
                            value=12,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block', 'marginRight': '2%'}),
                    
                    html.Div([
                        html.Label("Year", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='monthly-to-year',
                            options=[{'label': str(year), 'value': year} for year in range(2010, 2026)],
                            value=2025,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block'})
                ], style={'marginBottom': '10px'}),
                
                # All Range Button
                html.Button(
                    "📅 All Available Range",
                    id='monthly-all-range-btn',
                    n_clicks=0,
                    style={
                        'width': '100%',
                        'padding': '8px',
                        'fontSize': '10px',
                        'backgroundColor': '#6c757d',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '3px',
                        'cursor': 'pointer',
                        'marginBottom': '10px'
                    }
                )
            ], style={'marginBottom': '15px'}),
            
            html.Button(
                '📊 Calculate Monthly',
                id='calc-monthly-btn',
                n_clicks=0,
                title='Calculate yearly statistics and seasonal patterns',
                style={
                    'width': '100%',
                    'padding': '12px',
                    'fontSize': '12px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#28a745',
                    'color': 'white',
                    'border': 'none',
                    'borderRadius': '5px',
                    'cursor': 'pointer',
                    'textTransform': 'uppercase',
                    'letterSpacing': '0.5px'
                }
            )
        ], style={
            'padding': '15px',
            'backgroundColor': '#ffffff',
            'border': '2px solid #e9ecef',
            'borderRadius': '8px',
            'marginBottom': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        
        # Weekly Analysis Section
        html.Div([
            html.H4("📅 WEEKLY ANALYSIS", style={
                'color': '#2c3e50',
                'fontWeight': 'bold',
                'marginBottom': '15px',
                'textAlign': 'center',
                'fontSize': '14px',
                'textTransform': 'uppercase',
                'letterSpacing': '0.5px'
            }),
            html.Small("💡 Analyze weekly statistics and day-of-week patterns", 
                      style={'color': '#666', 'fontSize': '10px', 'marginBottom': '15px', 'display': 'block'}),
            
            # Weekly Date Range
            html.Div([
                html.Label("📅 Weekly Range", style={'fontWeight': 'bold', 'marginBottom': '8px', 'fontSize': '12px'}),
                
                html.Div([
                    html.Div([
                        html.Label("From", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='weekly-from-month',
                            options=[
                                {'label': 'January', 'value': 1},
                                {'label': 'February', 'value': 2},
                                {'label': 'March', 'value': 3},
                                {'label': 'April', 'value': 4},
                                {'label': 'May', 'value': 5},
                                {'label': 'June', 'value': 6},
                                {'label': 'July', 'value': 7},
                                {'label': 'August', 'value': 8},
                                {'label': 'September', 'value': 9},
                                {'label': 'October', 'value': 10},
                                {'label': 'November', 'value': 11},
                                {'label': 'December', 'value': 12}
                            ],
                            value=1,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block', 'marginRight': '2%'}),
                    
                    html.Div([
                        html.Label("Year", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='weekly-from-year',
                            options=[{'label': str(year), 'value': year} for year in range(2010, 2026)],
                            value=2025,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block'})
                ], style={'marginBottom': '10px'}),
                
                html.Div([
                    html.Div([
                        html.Label("To", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='weekly-to-month',
                            options=[
                                {'label': 'January', 'value': 1},
                                {'label': 'February', 'value': 2},
                                {'label': 'March', 'value': 3},
                                {'label': 'April', 'value': 4},
                                {'label': 'May', 'value': 5},
                                {'label': 'June', 'value': 6},
                                {'label': 'July', 'value': 7},
                                {'label': 'August', 'value': 8},
                                {'label': 'September', 'value': 9},
                                {'label': 'October', 'value': 10},
                                {'label': 'November', 'value': 11},
                                {'label': 'December', 'value': 12}
                            ],
                            value=12,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block', 'marginRight': '2%'}),
                    
                    html.Div([
                        html.Label("Year", style={'fontSize': '10px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.Dropdown(
                            id='weekly-to-year',
                            options=[{'label': str(year), 'value': year} for year in range(2010, 2026)],
                            value=2025,
                            clearable=False,
                            style={'fontSize': '10px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block'})
                ], style={'marginBottom': '10px'}),
                
                # All Range Button
                html.Button(
                    "📅 All Available Range",
                    id='weekly-all-range-btn',
                    n_clicks=0,
                    style={
                        'width': '100%',
                        'padding': '8px',
                        'fontSize': '10px',
                        'backgroundColor': '#6c757d',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '3px',
                        'cursor': 'pointer',
                        'marginBottom': '10px'
                    }
                )
            ], style={'marginBottom': '15px'}),
            
            html.Button(
                '📊 Calculate Weekly',
                id='calc-weekly-btn',
                n_clicks=0,
                title='Calculate weekly statistics and day-of-week analysis',
                style={
                    'width': '100%',
                    'padding': '12px',
                    'fontSize': '12px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#17a2b8',
                    'color': 'white',
                    'border': 'none',
                    'borderRadius': '5px',
                    'cursor': 'pointer',
                    'textTransform': 'uppercase',
                    'letterSpacing': '0.5px'
                }
            )
        ], style={
            'padding': '15px',
            'backgroundColor': '#ffffff',
            'border': '2px solid #e9ecef',
            'borderRadius': '8px',
            'marginBottom': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        
        # Day-of-Week Analysis Section
        html.Div([
            html.H4("📈 DAY-OF-WEEK ANALYSIS", style={
                'color': '#2c3e50',
                'fontWeight': 'bold',
                'marginBottom': '15px',
                'textAlign': 'center',
                'fontSize': '14px',
                'textTransform': 'uppercase',
                'letterSpacing': '0.5px'
            }),
            html.Small("💡 Analyze hourly statistics and HOD/LOD patterns", 
                      style={'color': '#666', 'fontSize': '10px', 'marginBottom': '15px', 'display': 'block'}),
            
            # Weekday Filters (inside the daily analysis box)
            html.Label("📅 Weekday Filters (applies to T-0)", style={
                'fontWeight': 'bold', 
                'marginBottom': '8px', 
                'fontSize': '12px'
            }),
            dcc.Checklist(
                id='weekday-filters',
                options=[
                    {'label': 'Monday', 'value': 'Monday'},
                    {'label': 'Tuesday', 'value': 'Tuesday'},
                    {'label': 'Wednesday', 'value': 'Wednesday'},
                    {'label': 'Thursday', 'value': 'Thursday'},
                    {'label': 'Friday', 'value': 'Friday'}
                ],
                value=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
                style={'fontSize': '10px', 'marginBottom': '15px'}
            ),
            
            html.Button(
                '📊 Calculate Daily',
                id='calc-daily-btn',
                n_clicks=0,
                title='Calculate hourly statistics and HOD/LOD analysis',
                style={
                    'width': '100%',
                    'padding': '12px',
                    'fontSize': '12px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#fd7e14',
                    'color': 'white',
                    'border': 'none',
                    'borderRadius': '5px',
                    'cursor': 'pointer',
                    'textTransform': 'uppercase',
                    'letterSpacing': '0.5px'
                }
            )
        ], style={
            'padding': '15px',
            'backgroundColor': '#ffffff',
            'border': '2px solid #e9ecef',
            'borderRadius': '8px',
            'marginBottom': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        
        # Hour-of-Day Analysis Section
        html.Div([
            html.H4("⏰ HOUR-OF-DAY ANALYSIS", style={
                'color': '#2c3e50',
                'fontWeight': 'bold',
                'marginBottom': '15px',
                'textAlign': 'center',
                'fontSize': '14px',
                'textTransform': 'uppercase',
                'letterSpacing': '0.5px'
            }),
            html.Small("💡 Analyze minute-level statistics for specific hour", 
                      style={'color': '#666', 'fontSize': '10px', 'marginBottom': '15px', 'display': 'block'}),
            
            # Minute Hour Selection (inside the hourly analysis box)
            html.Label("⏰ Minute Hour", style={
                'fontWeight': 'bold', 
                'marginBottom': '8px', 
                'fontSize': '12px'
            }),
            dcc.Dropdown(
                id='minute-hour-dropdown',
                options=[
                    {'label': 'All Hours', 'value': 'all'},
                    {'label': '9:00 AM', 'value': 9},
                    {'label': '10:00 AM', 'value': 10},
                    {'label': '11:00 AM', 'value': 11},
                    {'label': '12:00 PM', 'value': 12},
                    {'label': '1:00 PM', 'value': 13},
                    {'label': '2:00 PM', 'value': 14},
                    {'label': '3:00 PM', 'value': 15},
                    {'label': '4:00 PM', 'value': 16}
                ],
                value='all',
                clearable=False,
                style={'fontSize': '10px', 'marginBottom': '15px'}
            ),
            
        html.Button(
                '📊 Calculate Hourly',
                id='calc-hourly-btn',
                n_clicks=0,
                title='Calculate minute-level statistics for selected hour',
            style={
                'width': '100%',
                    'padding': '12px',
                    'fontSize': '12px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#dc3545',
                'color': 'white',
                'border': 'none',
                'borderRadius': '5px',
                'cursor': 'pointer',
                    'textTransform': 'uppercase',
                    'letterSpacing': '0.5px'
                }
            )
        ], style={
            'padding': '15px',
            'backgroundColor': '#ffffff',
            'border': '2px solid #e9ecef',
            'borderRadius': '8px',
            'marginBottom': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        
        # Filters Section
        html.Div([
            html.H4("🔍 FILTERS", style={
                'color': '#2c3e50',
                'fontWeight': 'bold',
                'marginBottom': '15px',
                'textAlign': 'center',
                'fontSize': '14px',
                'textTransform': 'uppercase',
                'letterSpacing': '0.5px'
            }),
            html.Small("💡 Apply filters to refine your analysis", 
                      style={'color': '#666', 'fontSize': '10px', 'marginBottom': '15px', 'display': 'block'}),
            
            dcc.Dropdown(
                id='filters',
                options=filter_options,
                value=['all'],
                multi=True,
                style={'fontSize': '10px', 'marginBottom': '15px'}
            ),
            
            # Volume Threshold
            html.Div([
                html.Label("📊 Volume Threshold:", style={'fontSize': '10px', 'marginBottom': '5px'}),
                dcc.Input(
                    id='vol-threshold',
                    type='number',
                    value=1000,
                    style={'fontSize': '10px', 'width': '100%'}
                )
            ], style={'marginBottom': '10px'}),
            
            # Percentage Threshold
            html.Div([
                html.Label("📈 Percentage Threshold:", style={'fontSize': '10px', 'marginBottom': '5px'}),
                dcc.Input(
                    id='pct-threshold',
                    type='number',
                    value=0.5,
                    step=0.1,
                    style={'fontSize': '10px', 'width': '100%'}
                )
            ], style={'marginBottom': '15px'}),
            
        html.Button(
                '📊 Calculate',
                id='calc-btn',
                n_clicks=0,
                title='Calculate statistics with applied filters',
            style={
                    'width': '100%',
                    'padding': '12px',
                    'fontSize': '12px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#007bff',
                'color': 'white',
                'border': 'none',
                    'borderRadius': '5px',
                    'cursor': 'pointer',
                    'textTransform': 'uppercase',
                    'letterSpacing': '0.5px'
            }
        )
    ], style={
            'padding': '15px',
            'backgroundColor': '#ffffff',
            'border': '2px solid #e9ecef',
            'borderRadius': '8px',
            'marginBottom': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        })
    ]
    
    return sidebar_content


def create_profile_layout():
    """Create the main profile page layout with sidebar."""
    
    layout = html.Div([
        # Responsive CSS styles - using inline styles instead
        # html.Style("""
        #     .sidebar {
        #         background-color: #f8f9fa;
        #         border-right: 1px solid #dee2e6;
        #         overflow-y: auto;
        #         padding: 20px;
        #     }
        #     .main-content {
        #         margin-left: 20%;
        #         padding: 20px;
        #         background-color: #ffffff;
        #     }
        #     @media (max-width: 768px) {
        #         .sidebar {
        #             width: 100% !important;
        #             position: relative !important;
        #         }
        #         .main-content {
        #             margin-left: 0 !important;
        #         }
        #     }
        # """),
        
        # Left sidebar (controls)
        html.Div(
            id='sidebar-left',
            className='sidebar',
            style={
                'position': 'fixed',
                'width': '20%',
                'height': '100vh',
                'overflowY': 'auto',
                'top': '0',
                'left': '0'
            },
            children=create_sidebar_content()
        ),
        
        # Right content area (charts)
        html.Div(
            id='main-content',
            className='main-content',
            style={
                'marginLeft': '20%',
                'padding': '20px',
                'backgroundColor': '#ffffff'
            },
            children=[
                # Progress indicator
                html.Div(id='progress-indicator', style={'display': 'none'}),
                
                # Summary box
                html.Div(id='summary-box', style={'marginBottom': '20px'}),
                
                # Total cases display
                html.Div(id='total-cases-display', style={'marginBottom': '20px'}),
                
                # HOD/LOD KPI Cards
                html.Div(id='hod-lod-kpi-cards', style={'marginBottom': '20px'}),
                
                # Monthly graphs container
                html.Div(id='monthly-graphs-container', children=[
                    dcc.Graph(id='month-avg', config={'displayModeBar': True}),
                    dcc.Graph(id='month-range', config={'displayModeBar': True}),
                ], style={'display': 'none'}),
                
                # Weekly graphs container
                html.Div(id='weekly-graphs-container', children=[
                    dcc.Graph(id='w-avg', config={'displayModeBar': True}),
                    dcc.Graph(id='w-var', config={'displayModeBar': True}),
                ], style={'display': 'none'}),
                
                # Daily graphs container
                html.Div(id='daily-graphs-container', children=[
                    dcc.Graph(id='h-avg', config={'displayModeBar': True}),
                    dcc.Graph(id='h-var', config={'displayModeBar': True}),
                    dcc.Graph(id='h-range', config={'displayModeBar': True}),
                    dcc.Graph(id='h-var-range', config={'displayModeBar': True}),
                ], style={'display': 'none'}),
                
                # Hourly graphs container
                html.Div(id='hourly-graphs-container', children=[
                    dcc.Graph(id='m-avg', config={'displayModeBar': True}),
                    dcc.Graph(id='m-var', config={'displayModeBar': True}),
                    dcc.Graph(id='m-range', config={'displayModeBar': True}),
                    dcc.Graph(id='m-var-range', config={'displayModeBar': True}),
                ], style={'display': 'none'}),
                
                # HOD/LOD analysis container
                html.Div(id='hod-lod-container', children=[
                    html.H3("HOD/LOD Analysis", style={'textAlign': 'center', 'marginBottom': '20px'}),
                    dcc.Graph(id='hod-survival', config={'displayModeBar': True}),
                    dcc.Graph(id='lod-survival', config={'displayModeBar': True}),
                    dcc.Graph(id='hod-heatmap', config={'displayModeBar': True}),
                    dcc.Graph(id='lod-heatmap', config={'displayModeBar': True}),
                    dcc.Graph(id='hod-rolling', config={'displayModeBar': True}),
                    dcc.Graph(id='lod-rolling', config={'displayModeBar': True}),
                ], style={'display': 'none'})
            ]
        )
    ])
    
    return layout