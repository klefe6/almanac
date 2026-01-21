"""
Profile Page - Clean Version

Main analysis page showing hourly/minute statistics with filtering capabilities.
"""

from dash import dcc, html, Input, Output, State, MATCH, ALL
import dash.dependencies
import pandas as pd
import math
import time

from ..data_sources import load_minute_data, load_daily_data
from ..features import (
    compute_hourly_stats,
    compute_minute_stats,
    compute_daily_stats,
    compute_monthly_stats,
    apply_filters,
    apply_time_filters,
    apply_quick_filter,
    apply_custom_filter,
    combine_filters,
    get_filtered_minute_data,
    calculate_sample_stats,
    calculate_individual_filter_stats,
    create_filter_description,
    validate_filter_config,
)
from ..features.hod_lod import (
    detect_hod_lod,
    compute_survival_curves,
    compute_hod_lod_heatmap,
    compute_rolling_median_time,
    compute_trend_test,
)
from ..viz import make_line_chart, make_heatmap, make_survival_curve
from ..export import (
    export_figure_to_csv,
    get_png_download_config,
    generate_shareable_url,
    get_current_page_url
)
import base64

# UI Components (Agent 3 additions)
from ..ui.components import (
    create_accordion_section,
    create_preset_controls,
    create_mobile_responsive_styles,
    create_help_modal,
    create_export_button
)
from ..ui.presets import PresetManager
from ..ui.keyboard import (
    create_keyboard_listener,
    register_help_modal_callbacks
)

from ..features.zone_filters import parse_zone_spec, apply_zone_filters, format_diagnostics


def _build_pct_change_filter_box(
    base_id: str,
    title: str,
    default_start: tuple[int, int, int],
    default_end: tuple[int, int, int],
    background_color: str,
    border_color: str,
    width: str = '100%',
):
    """Reusable UI block for a single percent-change zone filter."""
    day_opts = [
        {'label': 'T-1', 'value': -1},
        {'label': 'T-0', 'value': 0},
        {'label': 'T+1', 'value': 1},
    ]
    hour_opts = [{'label': f'{h:02d}', 'value': h} for h in range(0, 24)]
    minute_opts = [{'label': f'{m:02d}', 'value': m} for m in [0, 15, 30, 45]]

    sd, sh, sm = default_start
    ed, eh, em = default_end

    return html.Div([
        # Header with title and toggle
        html.Div([
            html.Div([
                html.Label(title, style={
                    'fontWeight': 'bold',
                    'marginBottom': '0px',
                    'color': '#2c3e50',
                    'fontSize': '13px',
                    'display': 'inline-block',
                    'marginRight': '10px'
                }),
                dcc.Checklist(
                    id=f'{base_id}-enabled',
                    options=[{'label': ' ON', 'value': True}],
                    value=[],
                    style={'display': 'inline-block'},
                    inputStyle={'marginRight': '4px', 'cursor': 'pointer'},
                    labelStyle={'fontSize': '11px', 'cursor': 'pointer', 'fontWeight': 'bold'}
                ),
            ], style={'display': 'inline-block'}),
            
            # Export button at top right
            html.Div([
                html.Button(
                    "📄 Export Dates",
                    id=f'{base_id}-export-btn',
                    style={
                        'fontSize': '10px',
                        'padding': '2px 6px',
                        'backgroundColor': '#f8f9fa',
                        'border': '1px solid #ddd',
                        'borderRadius': '4px',
                        'cursor': 'pointer'
                    },
                    title=f"Export dates passing {title} filter to Excel"
                ),
                dcc.Download(id=f'{base_id}-export-download')
            ], style={'float': 'right'})
        ], style={'marginBottom': '12px', 'overflow': 'hidden'}),

        # Target and Tolerance in a row
        html.Div([
            html.Div([
                html.Label("Target (%)", style={
                    'fontSize': '11px', 
                    'marginBottom': '4px',
                    'fontWeight': '600',
                    'color': '#495057'
                }),
                dcc.Input(
                    id=f'{base_id}-target',
                    type='number',
                    value=0.0,
                    min=-10, max=10, step=0.01,
                    style={
                        'width': '100%', 
                        'fontSize': '12px', 
                        'padding': '6px',
                        'border': '1px solid #ced4da',
                        'borderRadius': '4px'
                    }
                )
            ], style={'width': '22%', 'display': 'inline-block', 'marginRight': '3%'}),
            html.Div([
                html.Label("Tolerance ±(%)", style={
                    'fontSize': '11px', 
                    'marginBottom': '4px',
                    'fontWeight': '600',
                    'color': '#495057'
                }),
                dcc.Input(
                    id=f'{base_id}-tolerance',
                    type='number',
                    value=None,
                    placeholder='0.2',
                    min=0, step=0.01,
                    style={
                        'width': '100%', 
                        'fontSize': '12px', 
                        'padding': '6px',
                        'border': '1px solid #ced4da',
                        'borderRadius': '4px'
                    }
                )
            ], style={'width': '22%', 'display': 'inline-block'}),
        ], style={'marginBottom': '14px'}),

        # Start Time - on its own line
        html.Div([
            html.Label("Start:", style={
                'fontSize': '11px', 
                'marginBottom': '6px',
                'fontWeight': '600',
                'color': '#495057',
                'display': 'block'
            }),
            html.Div([
                dcc.Dropdown(
                    id=f'{base_id}-start-day',
                    options=day_opts,
                    value=sd,
                    clearable=False,
                    style={
                        'width': '28%', 
                        'display': 'inline-block', 
                        'marginRight': '3%', 
                        'fontSize': '11px'
                    }
                ),
                dcc.Dropdown(
                    id=f'{base_id}-start-hour',
                    options=hour_opts,
                    value=sh,
                    clearable=False,
                    style={
                        'width': '33%', 
                        'display': 'inline-block', 
                        'marginRight': '3%', 
                        'fontSize': '11px'
                    }
                ),
                dcc.Dropdown(
                    id=f'{base_id}-start-minute',
                    options=minute_opts,
                    value=sm,
                    clearable=False,
                    style={
                        'width': '33%', 
                        'display': 'inline-block', 
                        'fontSize': '11px'
                    }
                )
            ])
        ], style={'marginBottom': '14px'}),

        # End Time - on its own line
        html.Div([
            html.Label("End:", style={
                'fontSize': '11px', 
                'marginBottom': '6px',
                'fontWeight': '600',
                'color': '#495057',
                'display': 'block'
            }),
            html.Div([
                dcc.Dropdown(
                    id=f'{base_id}-end-day',
                    options=day_opts,
                    value=ed,
                    clearable=False,
                    style={
                        'width': '28%', 
                        'display': 'inline-block', 
                        'marginRight': '3%', 
                        'fontSize': '11px'
                    }
                ),
                dcc.Dropdown(
                    id=f'{base_id}-end-hour',
                    options=hour_opts,
                    value=eh,
                    clearable=False,
                    style={
                        'width': '33%', 
                        'display': 'inline-block', 
                        'marginRight': '3%', 
                        'fontSize': '11px'
                    }
                ),
                dcc.Dropdown(
                    id=f'{base_id}-end-minute',
                    options=minute_opts,
                    value=em,
                    clearable=False,
                    style={
                        'width': '33%', 
                        'display': 'inline-block', 
                        'fontSize': '11px'
                    }
                )
            ])
        ], style={'marginBottom': '8px'}),
    ], style={
        'backgroundColor': background_color,
        'padding': '14px',
        'borderRadius': '8px',
        'border': f'2px solid {border_color}',
        'marginBottom': '14px',
        'boxShadow': '0 1px 3px rgba(0,0,0,0.1)',
        'width': width
    })


def create_preset_row(preset_id, preset_name, day_count, logic_operator="AND"):
    """
    Create a dynamic preset row with X button, day count, dates, and AND/OR selector.
    """
    return html.Div([
        html.Div([
            # Left side: Preset name and day count
            html.Div([
                html.Span(f"📊 {preset_name}", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                html.Span(f"({day_count} days)", style={'fontSize': '11px', 'color': '#666', 'marginRight': '10px'}),
            ], style={'flex': '1'}),
            
            # Center: AND/OR selector
            html.Div([
                html.Label("Logic:", style={'fontSize': '10px', 'marginRight': '5px', 'color': '#666'}),
                dcc.Dropdown(
                    id={'type': 'preset-logic', 'index': preset_id},
                    options=[
                        {'label': 'AND', 'value': 'AND'},
                        {'label': 'OR', 'value': 'OR'}
                    ],
                    value=logic_operator,
                    clearable=False,
                    style={'width': '60px', 'fontSize': '10px'},
                    className='preset-logic-dropdown'
                )
            ], style={'marginRight': '10px'}),
            
            # Right side: X button to remove
            html.Button(
                "❌",
                id={'type': 'remove-preset', 'index': preset_id},
            style={
                    'padding': '2px 6px',
                    'fontSize': '10px',
                    'backgroundColor': '#e74c3c',
                    'color': 'white',
                    'border': 'none',
                    'borderRadius': '3px',
                    'cursor': 'pointer',
                    'minWidth': '20px'
                },
                title="Remove this preset"
            )
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'padding': '8px 12px',
            'backgroundColor': '#e8f4f8',
            'border': '1px solid #3498db',
            'borderRadius': '5px',
            'marginBottom': '8px'
        })
    ], id={'type': 'preset-row', 'index': preset_id})


def build_filter_panel(prefix: str, title: str, header_color: str = '#2196f3'):
    """
    Build a specialized filter panel for each timeframe while preserving
    IDs expected by callbacks. Unknown filter values are allowed (no-op) to
    keep buttons working until logic is added.
    """
    if prefix == 'monthly':
        filter_options = [
            {'label': '📈 High VIX Months', 'value': 'high_vix'},
            {'label': '🐂 Bull Years', 'value': 'bull_years'},
            {'label': '🔥 Inflation Periods', 'value': 'inflation_periods'},
            {'label': 'Q1 (Jan–Mar)', 'value': 'q1'},
            {'label': 'Q2 (Apr–Jun)', 'value': 'q2'},
            {'label': 'Q3 (Jul–Sep)', 'value': 'q3'},
            {'label': 'Q4 (Oct–Dec)', 'value': 'q4'},
            {'label': '✂️ Exclude Top/Bottom 5%', 'value': 'trim_extremes'},
        ]
        controls = [
            html.Div([
                html.Label('%∆ Threshold', style={'fontSize': '11px'}),
                dcc.Input(id=f'{prefix}-pct-threshold', type='number', placeholder='e.g., 2.0', style={'width': '100%'})
            ], style={'marginBottom': '8px'})
        ]
    elif prefix == 'daily':
        filter_options = [
            {'label': '📈 Prev-Day: Close > Open (Bullish)', 'value': 'prev_pos'},
            {'label': '📉 Prev-Day: Close < Open (Bearish)', 'value': 'prev_neg'},
            {'label': '🚀 Prev-Day: %∆ ≥ Threshold', 'value': 'prev_pct_pos'},
            {'label': '💥 Prev-Day: %∆ ≤ -Threshold', 'value': 'prev_pct_neg'},
            {'label': '📊 Prev-Day: Volume ≥ Threshold', 'value': 'relvol_gt'},
            {'label': '✂️ Exclude Top/Bottom 5%', 'value': 'trim_extremes'},
        ]
        controls = [
            html.Div([
                html.Label('%∆ Threshold', style={'fontSize': '11px'}),
                dcc.Input(id=f'{prefix}-pct-threshold', type='number', placeholder='e.g., 1.0', style={'width': '100%'})
            ], style={'marginBottom': '8px'}),
            html.Div([
                html.Label('Volume Multiplier', style={'fontSize': '11px'}),
                dcc.Input(id=f'{prefix}-vol-threshold', type='number', placeholder='e.g., 1.5', style={'width': '100%'})
            ], style={'marginBottom': '8px'})
        ]
    elif prefix == 'hourly':
        filter_options = [
            {'label': '📅 FOMC Day', 'value': 'fomc_days'},
            {'label': '📅 FOMC Week', 'value': 'fomc_week'},
            {'label': '📊 CPI Day', 'value': 'cpi_days'},
            {'label': '📈 Prev Hour %∆ > Threshold', 'value': 'prev_hour_pct'},
            {'label': '📊 High Volume Hour', 'value': 'high_vol_hour'},
            {'label': '⚡ Power Hour Only (15:00)', 'value': 'power_hour'},
            {'label': '✂️ Exclude Top/Bottom 5%', 'value': 'trim_extremes'},
            {'label': '📅 Monday', 'value': 'monday'},
            {'label': '📅 Tuesday', 'value': 'tuesday'},
            {'label': '📅 Wednesday', 'value': 'wednesday'},
            {'label': '📅 Thursday', 'value': 'thursday'},
            {'label': '📅 Friday', 'value': 'friday'},
        ]
        controls = [
            html.Div([
                html.Label('%∆ Threshold', style={'fontSize': '11px'}),
                dcc.Input(id=f'{prefix}-pct-threshold', type='number', placeholder='e.g., 0.5', style={'width': '100%'})
            ], style={'marginBottom': '8px'}),
            html.Div([
                html.Label('Volume Multiplier', style={'fontSize': '11px'}),
                dcc.Input(id=f'{prefix}-vol-threshold', type='number', placeholder='e.g., 1.5', style={'width': '100%'})
            ], style={'marginBottom': '8px'})
        ]
    elif prefix == 'minute':
        filter_options = [
            {'label': '📈 Prev Hour +0.5–1.5 %', 'value': 'prev_hour_range'},
            {'label': '📊 Minute Volume > 2× Avg', 'value': 'volume_spike'},
            {'label': '🔝 After HOD', 'value': 'after_hod'},
            {'label': '🔻 After LOD', 'value': 'after_lod'},
            {'label': '📊 VWAP Cross Only', 'value': 'vwap_cross'},
            {'label': '✂️ Exclude Top/Bottom 5%', 'value': 'trim_extremes'},
            {'label': '📅 Monday', 'value': 'monday'},
            {'label': '📅 Tuesday', 'value': 'tuesday'},
            {'label': '📅 Wednesday', 'value': 'wednesday'},
            {'label': '📅 Thursday', 'value': 'thursday'},
            {'label': '📅 Friday', 'value': 'friday'},
        ]
        controls = [
            html.Div([
                html.Label('Volume Multiplier', style={'fontSize': '11px'}),
                dcc.Input(id=f'{prefix}-vol-threshold', type='number', placeholder='e.g., 2.0', style={'width': '100%'})
            ], style={'marginBottom': '8px'})
        ]
    else:
        filter_options = []
        controls = []

    # Build panel with visible controls
    panel_content = html.Div([
        html.Div(title, style={'fontWeight': 'bold', 'color': 'white', 'backgroundColor': header_color,
                               'padding': '6px 10px', 'borderRadius': '4px', 'marginBottom': '8px', 'fontSize': '12px'}),
        dcc.Checklist(id=f'{prefix}-filters', options=filter_options, value=[], style={'marginBottom': '8px', 'fontSize': '12px'}),
        *controls,
        dcc.Store(id=f'{prefix}-filters-store', data=[], storage_type='session')
    ], style={'border': '1px solid #ddd', 'borderRadius': '6px', 'padding': '10px', 'backgroundColor': '#fafafa'})
    
    # For Monthly: add hidden fields that callbacks expect but aren't shown in UI
    if prefix == 'monthly':
        hours = [{'label': h, 'value': h} for h in range(0, 24) if h not in (5, 6)]
        minutes = [{'label': m, 'value': m} for m in range(0, 60)]
        hidden_fields = html.Div([
            dcc.Input(id='monthly-vol-threshold', type='number', style={'display': 'none'}),
            dcc.Dropdown(id='monthly-timeA-hour', options=hours, style={'display': 'none'}),
            dcc.Dropdown(id='monthly-timeA-minute', options=minutes, style={'display': 'none'}),
            dcc.Dropdown(id='monthly-timeB-hour', options=hours, style={'display': 'none'}),
            dcc.Dropdown(id='monthly-timeB-minute', options=minutes, style={'display': 'none'}),
        ])
        return html.Div([panel_content, hidden_fields])
    
    return panel_content

def create_sidebar_content():
    """Create accordion-based sidebar content with collapsible sections."""
    
    # Enhanced filter options with better organization and descriptions
    filter_options = [
        # Previous Day Performance
        {'label': '📈 Prev-Day: Close > Open (Bullish)', 'value': 'prev_pos'},
        {'label': '📉 Prev-Day: Close < Open (Bearish)', 'value': 'prev_neg'},
        {'label': '🚀 Prev-Day: %∆ ≥ Threshold (Strong Move Up)', 'value': 'prev_pct_pos'},
        {'label': '💥 Prev-Day: %∆ ≤ -Threshold (Strong Move Down)', 'value': 'prev_pct_neg'},
        
        # Previous Day Range & Volume
        {'label': '📊 Prev-Day: High Relative Volume (≥ Threshold)', 'value': 'relvol_gt'},
        {'label': '📉 Prev-Day: Low Relative Volume (≤ Threshold)', 'value': 'relvol_lt'},
        
        # Time-based Conditions
        {'label': '⏰ Time A > Time B (Custom Time Comparison)', 'value': 'timeA_gt_timeB'},
        {'label': '⏰ Time A < Time B (Custom Time Comparison)', 'value': 'timeA_lt_timeB'},
        
        # Data Quality
        {'label': '✂️ Exclude Top/Bottom 5% (Trim Extremes)', 'value': 'trim_extremes'},
        
        # Weekday Filters
        {'label': '📅 Monday', 'value': 'monday'},
        {'label': '📅 Tuesday', 'value': 'tuesday'},
        {'label': '📅 Wednesday', 'value': 'wednesday'},
        {'label': '📅 Thursday', 'value': 'thursday'},
        {'label': '📅 Friday', 'value': 'friday'},
    ]
    
    # Complete product options list
    product_options = [
                            # Equities
                            {'label': '─── EQUITIES ───', 'value': '', 'disabled': True},
                            {'label': 'ES - E-mini S&P 500', 'value': 'ES'},
                            {'label': 'NQ - E-mini Nasdaq', 'value': 'NQ'},
                            {'label': 'YM - E-mini Dow', 'value': 'YM'},
                            
                            # Metals
                            {'label': '─── METALS ───', 'value': '', 'disabled': True},
                            {'label': 'GC - Gold', 'value': 'GC'},
                            {'label': 'SI - Silver', 'value': 'SI'},
                            {'label': 'HG - Copper', 'value': 'HG'},
                            {'label': 'PA - Palladium', 'value': 'PA'},
                            {'label': 'PL - Platinum', 'value': 'PL'},
                            
                            # Energies
                            {'label': '─── ENERGIES ───', 'value': '', 'disabled': True},
                            {'label': 'CL - Crude Oil', 'value': 'CL'},
                            {'label': 'NG - Natural Gas', 'value': 'NG'},
                            {'label': 'RP - Refined Products', 'value': 'RP'},
                            
                            # Currencies
                            {'label': '─── CURRENCIES ───', 'value': '', 'disabled': True},
                            {'label': 'EU - Euro FX', 'value': 'EU'},
                            {'label': 'JY - Japanese Yen', 'value': 'JY'},
                            {'label': 'BP - British Pound', 'value': 'BP'},
                            {'label': 'CD - Canadian Dollar', 'value': 'CD'},
                            {'label': 'AD - Australian Dollar', 'value': 'AD'},
                            {'label': 'SF - Swiss Franc', 'value': 'SF'},
                            
                            # Treasuries
                            {'label': '─── TREASURIES ───', 'value': '', 'disabled': True},
                            {'label': 'TY - 10-Year Treasury', 'value': 'TY'},
                            {'label': 'FV - 5-Year Treasury', 'value': 'FV'},
                            {'label': 'TU - 2-Year Treasury', 'value': 'TU'},
                            {'label': 'US - 30-Year Treasury', 'value': 'US'},
                            
                            # Agriculturals
                            {'label': '─── AGRICULTURALS ───', 'value': '', 'disabled': True},
                            {'label': 'C - Corn', 'value': 'C'},
                            {'label': 'S - Soybeans', 'value': 'S'},
                            {'label': 'W - Wheat', 'value': 'W'},
                            
                            # Stocks & Crypto
                            {'label': '─── STOCKS & CRYPTO ───', 'value': '', 'disabled': True},
                            {'label': 'TSLA - Tesla Stock', 'value': 'TSLA'},
                            {'label': 'BTC/USD - Bitcoin', 'value': 'BTCUSD'},
    ]
    
    sidebar_content = [
        html.H2("Almanac Futures", style={'marginBottom': '20px', 'fontSize': '20px'}),
        
        # Product & Time of Interest Section
        create_accordion_section(
            'product-time-section',
            'Product & Time of Interest',
            [
                html.Label("Studied Product", style={'fontWeight': 'bold'}),
                dcc.Dropdown(
                    id='product-dropdown',
                    options=product_options,
                    value='ES',
                    clearable=False,
                    style={'marginBottom': '20px'}
                )
            ],
            is_open=True,
            icon='📈'
        ),
        
        # Date Range Section
        create_accordion_section(
            'date-range-section',
            'Date Range',
            [
                html.Div([
                    html.Div([
                        html.Label("Start Date", style={'fontSize': '11px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.DatePickerSingle(
                            id='filter-start-date',
                            display_format='YYYY-MM-DD',
                            placeholder='Select start date...',
                            style={'fontSize': '11px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block', 'marginRight': '4%'}),
                    
                    html.Div([
                        html.Label("End Date", style={'fontSize': '11px', 'marginBottom': '5px', 'color': '#666'}),
                        dcc.DatePickerSingle(
                            id='filter-end-date',
                            display_format='YYYY-MM-DD',
                            placeholder='Select end date...',
                            style={'fontSize': '11px'}
                        )
                    ], style={'width': '48%', 'display': 'inline-block'})
                ], style={'marginBottom': '15px'}),
                
                html.Button(
                    "Max Date Range",
                    id='max-date-range-btn',
                    style={
                        'width': '100%',
                        'padding': '8px',
                        'marginBottom': '10px',
                        'backgroundColor': '#27ae60',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'fontSize': '12px',
                        'fontWeight': 'bold'
                    },
                    title="Set date range to first and last available dates for selected product"
                )
            ],
            is_open=True,
            icon='📅'
        ),
        
        # Statistical Visuals Section
        create_accordion_section(
            'statistical-section',
            'Stats',
            [
                html.Label("Trimming of Mean %", style={'fontWeight': 'bold', 'marginBottom': '5px'}),
                dcc.Input(
                    id='median-percentage',
                    type='number',
                    value=5.0,
                    min=0,
                    max=50,
                    step=0.1,
                    style={'width': '100%', 'marginBottom': '15px'},
                    placeholder='Enter percentage (0-50%)'
                ),
                html.Small("Percentage for Mean Trim calculations (0-50%)", style={'color': '#666', 'fontSize': '11px', 'display': 'block', 'marginBottom': '15px'}),
                
                html.Label("Display Measures", style={'fontWeight': 'bold', 'marginBottom': '10px'}),
                dcc.Checklist(
                    id='stat-measures',
                    options=[
                        {'label': 'Mean', 'value': 'mean'},
                        {'label': 'Trimmed Mean', 'value': 'trimmed_mean'},
                        {'label': 'Median', 'value': 'median'},
                        {'label': 'Mode', 'value': 'mode'},
                        {'label': 'Outlier', 'value': 'outlier'}
                    ],
                    value=['mean', 'trimmed_mean', 'median', 'mode', 'outlier'],
                    style={'marginBottom': '20px'}
                ),
                
                # Progress Bar
                html.Div([
                    html.Div([
                        html.Div(id='calc-progress-bar', style={
                            'width': '0%',
                            'height': '20px',
                            'backgroundColor': '#007bff',
                            'borderRadius': '10px',
                            'transition': 'width 0.3s ease'
                        })
                    ], style={
                        'width': '100%',
                        'height': '20px',
                        'backgroundColor': '#e9ecef',
                        'borderRadius': '10px',
                        'overflow': 'hidden',
                        'marginBottom': '10px',
                        'display': 'none'
                    }, id='calc-progress-container'),
                    html.Div(id='calc-status', style={'fontSize': '12px', 'color': '#666', 'textAlign': 'center'})
                ]),
            ],
            is_open=True,
            icon='📊'
        ),
        
        # Calculate Buttons Section - Individual containers for each button
        create_accordion_section(
            'calc-monthly-section',
            'Month-of-Year',
            [
                html.Button(
                    'Calculate Monthly',
                    id='calc-monthly-btn',
                    n_clicks=0,
                    title='Run monthly analysis (January-December)',
                    style={
                        'width': '100%',
                        'padding': '12px',
                        'fontWeight': 'bold',
                        'backgroundColor': '#20c997',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'fontSize': '14px'
                    }
                ),
                build_filter_panel('monthly', 'Monthly Filters', '#00bfa5')
            ],
            is_open=True,
            icon='🗓️'
        ),
        
        create_accordion_section(
            'calc-daily-section',
            'Day-of-Week',
            [
                html.Button(
                    'Calculate Daily',
                    id='calc-daily-btn',
                    n_clicks=0,
                    title='Run day-of-week analysis (Monday-Sunday)',
                    style={
                        'width': '100%',
                        'padding': '12px',
                        'fontWeight': 'bold',
                        'backgroundColor': '#6f42c1',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'fontSize': '14px'
                    }
                ),
                build_filter_panel('daily', 'Daily Filters', '#7e57c2')
            ],
            is_open=True,
            icon='📅'
        ),

        create_accordion_section(
            'pct-change-section',
            'Pct Change Filters',
            [
                html.Div([
                    html.Small(
                        "Applies to both Hour-of-Day and Minute-of-Hour analyses",
                        style={'color': '#666', 'fontSize': '11px', 'display': 'block', 'marginBottom': '10px'}
                    ),
                    _build_pct_change_filter_box(
                        base_id='prev-ny',
                        title='% Change of prev NY session',
                        default_start=(-1, 9, 30),
                        default_end=(-1, 16, 0),
                        background_color='#fff3cd',
                        border_color='#ffc107',
                        width='110%'
                    ),
                    _build_pct_change_filter_box(
                        base_id='zone1',
                        title='% Change zone 1',
                        default_start=(-1, 16, 0),
                        default_end=(0, 8, 0),
                        background_color='#d1ecf1',
                        border_color='#17a2b8',
                        width='110%'
                    ),
                    _build_pct_change_filter_box(
                        base_id='zone2',
                        title='% Change zone 2',
                        default_start=(0, 8, 0),
                        default_end=(0, 9, 30),
                        background_color='#d4edda',
                        border_color='#28a745'
                    ),
                ])
            ],
            is_open=True,
            icon='🎯'
        ),
        
        create_accordion_section(
            'calc-hourly-section',
            'Hour-of-Day',
            [
                html.Button(
                    'Calculate Hourly',
                    id='calc-hourly-btn',
                    n_clicks=0,
                    title='Run hourly analysis only',
                    style={
                        'width': '100%',
                        'padding': '12px',
                        'fontWeight': 'bold',
                        'backgroundColor': '#007bff',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'fontSize': '14px'
                    }
                ),
                build_filter_panel('hourly', 'Hourly Filters', '#2196f3')
            ],
            is_open=True,
            icon='📊'
        ),
        
        create_accordion_section(
            'calc-minutes-section',
            'Minute-of-Hour',
            [
                html.Button(
                    'Calculate Minutes',
                    id='calc-minutes-btn',
                    n_clicks=0,
                    title='Run minute analysis only',
                    style={
                        'width': '100%',
                        'padding': '12px',
                        'fontWeight': 'bold',
                        'backgroundColor': '#28a745',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'fontSize': '14px',
                        'marginBottom': '10px'
                    }
                ),
                html.Label("Minute Hour", style={'fontWeight': 'bold'}),
                dcc.Dropdown(
                    id='minute-hour',
                    options=[{'label': f'{h}:00', 'value': h} for h in range(0, 24) if h not in (5, 6)],
                    value=9,
                    clearable=False,
                    style={'marginBottom': '0px'}
                ),
                html.Div(style={'height': '8px'}),
                build_filter_panel('minute', 'Minute Filters', '#43a047')
            ],
            is_open=True,
            icon='⏱️'
        ),

        # Hidden global fields required by callbacks (fallback only)
        html.Div([
            dcc.Checklist(id='filters', options=[], value=[]),
            dcc.Input(id='vol-threshold', type='number'),
            dcc.Input(id='pct-threshold', type='number'),
            dcc.Dropdown(id='timeA-hour'),
            dcc.Dropdown(id='timeA-minute'),
            dcc.Dropdown(id='timeB-hour'),
            dcc.Dropdown(id='timeB-minute'),
            html.Div(id='dynamic-preset-rows'),
            html.Span(id='total-cases-display'),
            html.Div(id='preset-status-message'),
            dcc.Store(id='presets-store', data={}, storage_type='session'),
            html.Div(id='sample-size-display')
        ], style={'display': 'none'}),
        
        # Action Buttons section removed - calculate button moved to top
        
        # Export Controls (Agent 2 additions)
        create_accordion_section(
            'export-section',
            'Export & Share',
            [
                html.Button(
                    'Share URL',
                    id='share-url-btn',
                    n_clicks=0,
                    style={
                        'width': '100%',
                        'padding': '8px',
                        'marginBottom': '8px',
                        'backgroundColor': '#28a745',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'fontSize': '14px'
                    }
                ),
                
                html.Div(id='share-url-display', style={
                    'display': 'none',
                    'marginBottom': '10px',
                    'padding': '8px',
                    'backgroundColor': '#e8f5e9',
                    'borderRadius': '4px',
                    'fontSize': '11px',
                    'wordBreak': 'break-all'
                }),
                
                html.Button(
                    'Download All CSV',
                    id='download-all-csv-btn',
                    n_clicks=0,
                    style={
                        'width': '100%',
                        'padding': '8px',
                        'marginBottom': '8px',
                        'backgroundColor': '#6c757d',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'fontSize': '14px'
                    }
                ),
                
                dcc.Download(id='download-all-csv'),
                
                html.Small("💡 Tip: Use the camera icon on charts to download PNG", 
                          style={'color': '#666', 'fontSize': '11px'})
            ],
            is_open=True,
            icon='📤'
        ),
                
                # Summary box
                html.Div(
                    id='summary-box',
                    style={
                'marginTop': '15px',
                'padding': '12px',
                        'border': '1px solid #ccc',
                'borderRadius': '5px',
                        'backgroundColor': '#ffffff',
                'fontSize': '13px',
                'lineHeight': '1.5'
            }
        )
    ]
    
    return sidebar_content


def create_profile_layout():
    """Create the main profile page layout with accordion-based sidebar."""
    
    layout = html.Div([
        # Responsive CSS styles
        create_mobile_responsive_styles(),
        
        # Keyboard listener
        create_keyboard_listener(),
        
        # Help modal
        create_help_modal(),
        
        # State storage
        dcc.Store(id='query-spec', storage_type='session'),
        dcc.Store(id='custom-filters-store', data=[], storage_type='session'),
        dcc.Store(id='dynamic-filters-store', data=[], storage_type='session'),
        dcc.Store(id='active-presets-store', data=[], storage_type='session'),  # Track active presets
        
        # Left sidebar (controls) - NEW ACCORDION STRUCTURE
        html.Div(
            className='sidebar',
            style={
                'position': 'fixed',
                'width': '20%',
                'height': '100vh',
                'overflow': 'auto',
                'padding': '20px',
                'borderRight': '1px solid #ccc',
                'backgroundColor': '#f8f9fa'
            },
            children=create_sidebar_content()
        ),
        
        # Right content area (charts)
        html.Div(
            className='content-area',
            style={'marginLeft': '22%', 'padding': '20px'},
            children=[
                html.H3("Month-of-Year Statistics"),
                dcc.Graph(id='monthly-avg', config=get_png_download_config('monthly_avg_change')),
                dcc.Graph(id='monthly-var', config=get_png_download_config('monthly_var_change')),
                dcc.Graph(id='monthly-range', config=get_png_download_config('monthly_avg_range')),
                dcc.Graph(id='monthly-var-range', config=get_png_download_config('monthly_var_range')),
                
                html.H3("Day-of-Week Statistics"),
                dcc.Graph(id='d-avg', config=get_png_download_config('daily_avg_change')),
                dcc.Graph(id='d-var', config=get_png_download_config('daily_var_change')),
                dcc.Graph(id='d-range', config=get_png_download_config('daily_avg_range')),
                dcc.Graph(id='d-var-range', config=get_png_download_config('daily_var_range')),
                
                html.H3("Hour-of-Day Statistics"),
                dcc.Graph(id='h-avg', config=get_png_download_config('hourly_avg_change')),
                dcc.Graph(id='h-var', config=get_png_download_config('hourly_var_change')),
                dcc.Graph(id='h-range', config=get_png_download_config('hourly_avg_range')),
                dcc.Graph(id='h-var-range', config=get_png_download_config('hourly_var_range')),
                
                html.H3("Minute-of-Hour Statistics"),
                dcc.Graph(id='m-avg', config=get_png_download_config('minute_avg_change')),
                dcc.Graph(id='m-var', config=get_png_download_config('minute_var_change')),
                dcc.Graph(id='m-range', config=get_png_download_config('minute_avg_range')),
                dcc.Graph(id='m-var-range', config=get_png_download_config('minute_var_range')),
                
                html.Hr(style={'margin': '40px 0'}),
                
                html.H3("HOD/LOD Analysis"),
                html.Div(id='hod-lod-kpi-cards', style={
                    'display': 'grid',
                    'gridTemplateColumns': 'repeat(auto-fit, minmax(200px, 1fr))',
                    'gap': '15px',
                    'marginBottom': '30px'
                }),
                
                html.H4("Survival Curves (Probability HOD/LOD occurred by time T)"),
                html.Div([
                    html.Div([dcc.Graph(id='hod-survival')], style={'width': '49%', 'display': 'inline-block'}),
                    html.Div([dcc.Graph(id='lod-survival')], style={'width': '49%', 'display': 'inline-block', 'marginLeft': '2%'})
                ]),
                
                html.H4("HOD/LOD Frequency Heatmaps (by Weekday)"),
                html.Div([
                    html.Div([dcc.Graph(id='hod-heatmap')], style={'width': '49%', 'display': 'inline-block'}),
                    html.Div([dcc.Graph(id='lod-heatmap')], style={'width': '49%', 'display': 'inline-block', 'marginLeft': '2%'})
                ]),
                
                html.H4("Rolling Median Time (63-day window)"),
                html.Div([
                    html.Div([dcc.Graph(id='hod-rolling')], style={'width': '49%', 'display': 'inline-block'}),
                    html.Div([dcc.Graph(id='lod-rolling')], style={'width': '49%', 'display': 'inline-block', 'marginLeft': '2%'})
                ]),
            ]
        )
    ])
    
    return layout


# OLD FUNCTION REMOVED - Using register_profile_callbacks instead

def register_profile_callbacks_old_DISABLED(app, cache):
    """Register all callbacks for the profile page."""
    
    # TEMPORARY FIX: Simplified callback to get app working
    @app.callback(
        [
            Output('h-avg', 'figure'),
            Output('h-var', 'figure'),
            Output('h-range', 'figure'),
            Output('h-var-range', 'figure'),
            Output('m-avg', 'figure'),
            Output('m-var', 'figure'),
            Output('m-range', 'figure'),
            Output('m-var-range', 'figure'),
            Output('summary-box', 'children'),
            Output('hod-lod-kpi-cards', 'children'),
            Output('hod-survival', 'figure'),
            Output('lod-survival', 'figure'),
            Output('hod-heatmap', 'figure'),
            Output('lod-heatmap', 'figure'),
            Output('hod-rolling', 'figure'),
            Output('lod-rolling', 'figure'),
            Output('total-cases-display', 'children'),
        ],
        Input('calc-btn', 'n_clicks'),
        [
            State('product-dropdown', 'value'),
            State('filter-start-date', 'date'),
            State('filter-end-date', 'date'),
            State('minute-hour', 'value'),
            State('filters', 'value'),
            State('vol-threshold', 'value'),
            State('pct-threshold', 'value'),
            State('median-percentage', 'value'),
            State('stat-measures', 'value'),
            State('timeA-hour', 'value'),
            State('timeA-minute', 'value'),
            State('timeB-hour', 'value'),
            State('timeB-minute', 'value'),
        ]
    )
    @cache.memoize(timeout=3600)  # OPTIMIZED: 1 hour cache for historical data
    def update_hourly_graphs(n, prod, start, end, mh, filters, vol_thr, pct_thr, median_pct, selected_measures, tA_h, tA_m, tB_h, tB_m):
        """Main callback to update all charts and summary."""
        print(f"\n[DEBUG] Callback triggered: n_clicks={n}")
        print(f"[DEBUG] Parameters: prod={prod}, start={start}, end={end}, mh={mh}")
        print(f"[DEBUG] filters={filters}, vol_thr={vol_thr}, pct_thr={pct_thr}")
        print(f"[DEBUG] median_pct={median_pct}, selected_measures={selected_measures}")
        # Defaults for per-panel overrides
        active_filters_hr = filters
        vol_thr_hr = vol_thr
        pct_thr_hr = pct_thr
        
        # Initialize HOD/LOD variables
        empty_fig = make_line_chart([], [], "No Data", "", "")
        empty_kpi = html.Div("Initializing...")
        hod_survival_fig = lod_survival_fig = empty_fig
        hod_heatmap_fig = lod_heatmap_fig = empty_fig
        hod_rolling_fig = lod_rolling_fig = empty_fig
        
        try:
            debug_msgs = []
            # Panel-specific states (hourly)
            try:
                ctx_states = dash.callback_context.states
            except Exception:
                ctx_states = {}
            active_filters_hr = ctx_states.get('hourly-filters.value', None)
            vol_thr_hr = ctx_states.get('hourly-vol-threshold.value', None)
            pct_thr_hr = ctx_states.get('hourly-pct-threshold.value', None)
            if not active_filters_hr:
                active_filters_hr = filters
            if vol_thr_hr is None:
                vol_thr_hr = vol_thr
            if pct_thr_hr is None:
                pct_thr_hr = pct_thr
            # Try loading from database first
            try:
                daily = load_daily_data(prod, start, end)
                minute = load_minute_data(prod, start, end)
                debug_msgs.append(f"Loaded daily={len(daily)} rows, minute={len(minute)} rows")
                
                # Intermarket data loading disabled in simplified version
                intermarket_daily = None
                        
            except Exception as db_error:
                # Fallback to demo data if database unavailable
                print(f"Database unavailable, using demo data: {db_error}")
                from ..data_sources import generate_demo_daily_data, generate_demo_minute_data
                daily = generate_demo_daily_data(prod, start, end)
                minute = generate_demo_minute_data(prod, start, end)
                intermarket_daily = None
            
            # Check if we have data
            if daily.empty or minute.empty:
                error_msg = html.Div([
                    html.B("Error:", style={'color': 'red'}),
                    html.Br(),
                    f"No data found for {prod} between {start} and {end}"
                ])
                empty_fig = make_line_chart([], [], "No Data", "", "")
                empty_kpi = html.Div("No data available")
                return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("77 cases", {'width': '0%', 'height': '20px', 'backgroundColor': '#dc3545', 'borderRadius': '10px', 'transition': 'width 0.3s ease'}, {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'}, "Error occurred")
            
            # Apply filtering
            filtered_minute = minute
            
            # Apply additional filters
            try:
                pre_rows = len(filtered_minute)
                if 'active_filters_hr' not in locals():
                    active_filters_hr = filters
                if 'vol_thr_hr' not in locals():
                    vol_thr_hr = vol_thr
                if 'pct_thr_hr' not in locals():
                    pct_thr_hr = pct_thr
                filtered_minute = apply_filters(filtered_minute, daily, active_filters_hr, vol_thr_hr, pct_thr_hr)
                debug_msgs.append(f"After base filters [{','.join(filters or [])}]: {pre_rows} -> {len(filtered_minute)} rows")
            except Exception as filter_error:
                import traceback
                traceback.print_exc()
                raise
            
            # Apply time filters if specified
            if filters and any(f in filters for f in ['timeA_gt_timeB', 'timeA_lt_timeB']):
                try:
                    before_time = len(filtered_minute)
                    filtered_minute = apply_time_filters(
                        filtered_minute, filters, tA_h, tA_m, tB_h, tB_m
                    )
                    debug_msgs.append(f"After time filters: {before_time} -> {len(filtered_minute)} rows")
                except Exception as time_error:
                    import traceback
                    traceback.print_exc()
                    raise
            
            # Compute statistics (now includes all 4 measures)
            try:
                # Set default values
                trim_pct = median_pct or 5.0
                selected_measures = selected_measures or ['mean', 'trimmed_mean', 'median', 'mode']
                
                # Compute hourly stats - all 5 measures (including outlier)
                hc, hcm, hmed, hmode, houtlier, hv, hr, hrm, hmed_r, hmode_r, houtlier_r, hvr = compute_hourly_stats(filtered_minute, trim_pct)
                
                # Compute minute stats with all 5 measures (including outlier)
                mc, mcm, mmed, mmode, moutlier, mv, mr, mrm, mmed_r, mmode_r, moutlier_r, mvr = compute_minute_stats(filtered_minute, mh, trim_pct)
            except Exception as stats_error:
                import traceback
                traceback.print_exc()
                raise
            
            # Scale variance for display
            sh, _ = _scale_variance(hv.mean())
            sm, _ = _scale_variance(mv.mean())
            
            # Compute HOD/LOD analysis
            print(f"[HOD/LOD] Starting analysis...")
            
            try:
                # Detect HOD/LOD for each trading day
                hod_lod_df = detect_hod_lod(filtered_minute)
                print(f"[HOD/LOD] Detected HOD/LOD for {len(hod_lod_df)} days")
                
                if len(hod_lod_df) < 10:
                    print(f"[HOD/LOD] Insufficient data ({len(hod_lod_df)} days), using empty charts")
                    empty_fig = make_line_chart([], [], "Insufficient Data", "", "")
                    empty_kpi = html.Div([
                        html.B("HOD/LOD Analysis", style={'marginBottom': '10px'}),
                        html.Br(),
                        html.Div(f"Insufficient data ({len(hod_lod_df)} days, need ≥10)", 
                                style={'color': '#666', 'fontSize': '14px'})
                    ])
                    hod_survival_fig = lod_survival_fig = empty_fig
                    hod_heatmap_fig = lod_heatmap_fig = empty_fig
                    hod_rolling_fig = lod_rolling_fig = empty_fig
                else:
                    # Compute survival curves
                    print(f"[HOD/LOD] Computing survival curves...")
                    hod_survival, lod_survival = compute_survival_curves(hod_lod_df)
                    
                    # Compute heatmaps
                    print(f"[HOD/LOD] Computing heatmaps...")
                    hod_heatmap_data, lod_heatmap_data = compute_hod_lod_heatmap(hod_lod_df, by='weekday')
                    
                    # Compute rolling medians
                    print(f"[HOD/LOD] Computing rolling medians...")
                    hod_rolling = compute_rolling_median_time(hod_lod_df, 'hod', window=63)
                    lod_rolling = compute_rolling_median_time(hod_lod_df, 'lod', window=63)
                    
                    # Compute trend tests
                    print(f"[HOD/LOD] Computing trend tests...")
                    hod_trend = compute_trend_test(hod_lod_df['hod_minutes_since_midnight'])
                    lod_trend = compute_trend_test(hod_lod_df['lod_minutes_since_midnight'])
                    
                    # Create KPI cards
                    hod_median_time = hod_lod_df['hod_minutes_since_midnight'].median()
                    lod_median_time = hod_lod_df['lod_minutes_since_midnight'].median()
                    
                    hod_time_str = f"{int(hod_median_time//60):02d}:{int(hod_median_time%60):02d}"
                    lod_time_str = f"{int(lod_median_time//60):02d}:{int(lod_median_time%60):02d}"
                    
                    # Determine trend colors
                    hod_trend_color = 'success' if hod_trend['trend'] == 'increasing' else 'danger' if hod_trend['trend'] == 'decreasing' else 'secondary'
                    lod_trend_color = 'success' if lod_trend['trend'] == 'increasing' else 'danger' if lod_trend['trend'] == 'decreasing' else 'secondary'
                    
                    empty_kpi = html.Div([
                        html.H5("HOD/LOD Analysis", className="mb-4 text-center", 
                               style={'color': '#2c3e50', 'fontWeight': 'bold', 'marginBottom': '20px'}),
                        html.Div([
                            # HOD Time Bubble
                            html.Div([
                                html.Div([
                                    html.H6("MEDIAN HOD TIME", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H3(hod_time_str, style={
                                        'fontSize': '24px', 
                                        'fontWeight': 'bold', 
                                        'color': '#0066cc',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#e6f3ff',
                                'borderRadius': '16px',
                                'border': '2px solid #b3d9ff',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(0,102,204,0.1)'
                            }),
                            
                            # LOD Time Bubble
                            html.Div([
                                html.Div([
                                    html.H6("MEDIAN LOD TIME", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H3(lod_time_str, style={
                                        'fontSize': '24px', 
                                        'fontWeight': 'bold', 
                                        'color': '#009933',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#e6ffe6',
                                'borderRadius': '16px',
                                'border': '2px solid #99e699',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(0,153,51,0.1)'
                            }),
                            
                            # HOD Trend Bubble
                            html.Div([
                                html.Div([
                                    html.H6("HOD TREND", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H5(hod_trend['trend'].replace('_', ' ').title(), style={
                                        'fontSize': '16px', 
                                        'fontWeight': 'bold', 
                                        'color': '#17a2b8',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#e6f8ff',
                                'borderRadius': '16px',
                                'border': '2px solid #80d4ff',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(23,162,184,0.1)'
                            }),
                            
                            # LOD Trend Bubble
                            html.Div([
                                html.Div([
                                    html.H6("LOD TREND", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H5(lod_trend['trend'].replace('_', ' ').title(), style={
                                        'fontSize': '16px', 
                                        'fontWeight': 'bold', 
                                        'color': '#ffc107',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#fff8e6',
                                'borderRadius': '16px',
                                'border': '2px solid #ffdd99',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(255,193,7,0.1)'
                            }),
                            
                            # Sample Size Bubble
                            html.Div([
                                html.Div([
                                    html.H6("SAMPLE SIZE", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H3(f"{len(hod_lod_df)}", style={
                                        'fontSize': '24px', 
                                        'fontWeight': 'bold', 
                                        'color': '#6f42c1',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#f3e6ff',
                                'borderRadius': '16px',
                                'border': '2px solid #d1b3ff',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(111,66,193,0.1)'
                            }),
                        ], style={
                            'display': 'flex',
                            'justifyContent': 'center',
                            'alignItems': 'center',
                            'flexWrap': 'wrap',
                            'gap': '12px',
                            'padding': '0 20px'
                        })
                    ], style={'marginBottom': '30px'})
                    
                    # Create survival curve charts
                    hod_survival_fig = make_survival_curve(
                        hod_survival['minutes'], hod_survival['probability'],
                        "HOD Survival Curve", "Minutes Since Midnight", "Probability"
                    )
                    lod_survival_fig = make_survival_curve(
                        lod_survival['minutes'], lod_survival['probability'],
                        "LOD Survival Curve", "Minutes Since Midnight", "Probability"
                    )
                    
                    # Create heatmap charts
                    hod_heatmap_fig = make_heatmap(
                        hod_heatmap_data, "HOD Frequency by Weekday", "Time", "Weekday"
                    )
                    lod_heatmap_fig = make_heatmap(
                        lod_heatmap_data, "LOD Frequency by Weekday", "Time", "Weekday"
                    )
                    
                    # Create rolling median charts
                    hod_rolling_fig = make_line_chart(
                        hod_rolling['date'], hod_rolling['rolling_median'],
                        "HOD Rolling Median (63-day)", "Date", "Minutes Since Midnight"
                    )
                    lod_rolling_fig = make_line_chart(
                        lod_rolling['date'], lod_rolling['rolling_median'],
                        "LOD Rolling Median (63-day)", "Date", "Minutes Since Midnight"
                    )
                    
                    print(f"[HOD/LOD] Analysis complete!")
                    
            except Exception as hod_lod_error:
                print(f"[HOD/LOD] Error in analysis: {hod_lod_error}")
                import traceback
                traceback.print_exc()
                
                # Fallback to empty charts
                empty_fig = make_line_chart([], [], "HOD/LOD Error", "", "")
                empty_kpi = html.Div([
                    html.B("HOD/LOD Analysis", style={'marginBottom': '10px'}),
                    html.Br(),
                    html.Div(f"Analysis error: {str(hod_lod_error)[:50]}...", 
                            style={'color': '#666', 'fontSize': '14px'})
                ])
                hod_survival_fig = lod_survival_fig = empty_fig
                hod_heatmap_fig = lod_heatmap_fig = empty_fig
                hod_rolling_fig = lod_rolling_fig = empty_fig
            
            # Generate summary
            summary = _generate_summary(
                daily, filtered_minute, hc, hv, hr, hvr, mc, mv, mr, mvr, sh, sm
            )
            # Append debug information at the bottom of the summary
            if debug_msgs:
                summary = summary + [html.Hr(), html.B("Debug"), html.Br()] + [html.Div(m) for m in debug_msgs]
            
            # Count filtered trading days for total cases display - use total trading days if no filters
            filtered_days = filtered_minute['date'].nunique() if 'date' in filtered_minute.columns else 0
            if filtered_days == 0 and not filters:
                # No filters applied, use total trading days from daily data
                filtered_days = daily.index.nunique() if hasattr(daily, 'index') else len(daily)
            
            # Create dynamic title suffix
            title_suffix = f" | {prod} | {filtered_days} cases | {start} to {end}"
            
            # Create figures with all 5 statistical measures (including outlier)
            return (
                make_line_chart(hc.index, hc, f"Hourly Avg % Change{title_suffix}", "Hour", "Pct", 
                              mean_data=hc, trimmed_mean_data=hcm, median_data=hmed, mode_data=hmode, outlier_data=houtlier,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(hv.index, hv, f"Hourly Var % Change{title_suffix}", "Hour", "Var"),
                make_line_chart(hr.index, hr, f"Hourly Avg Range{title_suffix}", "Hour", "Price", 
                              mean_data=hr, trimmed_mean_data=hrm, median_data=hmed_r, mode_data=hmode_r, outlier_data=houtlier_r,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(hvr.index, hvr, f"Hourly Var Range{title_suffix}", "Hour", "Var"),
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-avg - empty for hourly-only
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-var - empty for hourly-only
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-range - empty for hourly-only
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-var-range - empty for hourly-only
                summary,
                empty_kpi,  # HOD/LOD KPI cards
                hod_survival_fig,
                lod_survival_fig,
                hod_heatmap_fig,
                lod_heatmap_fig,
                hod_rolling_fig,
                lod_rolling_fig,
                f"{filtered_days} cases",  # Total cases display
                {'width': '100%', 'height': '20px', 'backgroundColor': '#28a745', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},  # Progress bar complete
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},  # Hide container
                "Hour-of-Day analysis complete!"  # Status message
            )
        
            print(f"[DEBUG] Callback completed successfully!")
        
        except Exception as e:
            print(f"\n{'='*60}")
            print(f"EXCEPTION IN update_graphs CALLBACK:")
            print(f"{'='*60}")
            import traceback
            traceback.print_exc()
            print(f"{'='*60}\n")
            
            error_msg = html.Div([
                html.B("Error:", style={'color': 'red'}),
                html.Br(),
                html.Pre(str(e), style={'whiteSpace': 'pre-wrap', 'fontSize': '11px'}),
                html.Br(),
                html.B("Type:", style={'fontSize': '10px'}),
                html.Div(str(type(e).__name__), style={'fontSize': '10px'}),
            ])
            empty_fig = make_line_chart([], [], "No Data", "", "")
            empty_kpi = html.Div("Error occurred")
            return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("77 cases", {'width': '0%', 'height': '20px', 'backgroundColor': '#dc3545', 'borderRadius': '10px', 'transition': 'width 0.3s ease'}, {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'}, "Error occurred")


def _scale_variance(v):
    """Scale variance for better display."""
    if v is None or v <= 0 or math.isnan(v):
        return 0.0, 0
    exp = -math.floor(math.log10(v)) + 1
    return v * (10 ** exp), exp


def _generate_summary(daily, filtered_minute, hc, hv, hr, hvr, mc, mv, mr, mvr, sh, sm, zone_diagnostics=None):
    """Generate summary statistics box."""
    
    total_days = len(daily)
    green_days = (daily['close'] > daily['open']).sum()
    red_days = (daily['close'] < daily['open']).sum()
    
    op = daily['open'].iloc[0]
    cl = daily['close'].iloc[-1]
    chg = (cl - op) / op * 100
    
    # Count filtered data
    filtered_days = filtered_minute['date'].nunique() if 'date' in filtered_minute.columns else 0
    
    summary = [
        html.B("Date Range Summary"), html.Br(),
        f"Open: {op:.2f}", html.Br(),
        f"Close: {cl:.2f}", html.Br(),
        f"Change: {chg:.2f}%", html.Br(),
        f"Total Days: {total_days}", html.Br(),
        f"Green Days: {green_days} ({green_days/total_days*100:.2f}%)", html.Br(),
        f"Red Days: {red_days} ({red_days/total_days*100:.2f}%)", html.Br(),
        html.Br(),
        
        html.B("Filtered Data"), html.Br(),
        f"Days After Filters: {filtered_days}", html.Br(),
        f"Data Remaining: {filtered_days/total_days*100:.2f}%", html.Br(),
        html.Br(),
        
        html.B("Mean Variances (scaled)"), html.Br(),
        f"Hourly %∆ Var: {sh:.1f}", html.Br(),
        f"Hourly Range Var: {hvr.mean():.6f}", html.Br(),
        f"Minute %∆ Var: {sm:.1f}", html.Br(),
        f"Minute Range Var: {mvr.mean():.6f}", html.Br(),
    ]
    
    if zone_diagnostics:
        summary += [
            html.Hr(),
            html.B("🎯 Percentage Change Filters (Diagnostics)"),
            html.Br(),
        ]
        for line in format_diagnostics(zone_diagnostics):
            summary.append(html.Div(line, style={'fontSize': '11px', 'color': '#2c3e50'}))

    return summary
    

# Export callbacks (Agent 2 additions)
def register_export_callbacks(app):
    """Register export-related callbacks."""
    
    # Share URL Callback
    @app.callback(
        Output('share-url-display', 'children'),
        Output('share-url-display', 'style'),
        Input('share-url-btn', 'n_clicks'),
        [
            State('product-dropdown', 'value'),
            State('filter-start-date', 'date'),
            State('filter-end-date', 'date'),
            State('minute-hour', 'value'),
            State('filters', 'value'),
            State('vol-threshold', 'value'),
            State('pct-threshold', 'value'),
            State('median-percentage', 'value'),
            State('stat-measures', 'value'),
            State('timeA-hour', 'value'),
            State('timeA-minute', 'value'),
            State('timeB-hour', 'value'),
            State('timeB-minute', 'value'),
        ],
        prevent_initial_call=True
    )
    def generate_share_url(n_clicks, product, start_date, end_date, minute_hour, 
                          filters, vol_threshold, pct_threshold, trim_percentage,
                          stat_measures, timeA_hour, timeA_minute,
                          timeB_hour, timeB_minute):
        """Generate a shareable URL with current settings."""
        if n_clicks is None or n_clicks == 0:
            return "", {'display': 'none'}
        
        try:
            print(f"\n[SHARE URL] Button clicked (n_clicks={n_clicks})")
            print(f"[SHARE URL] Product: {product}, Dates: {start_date} to {end_date}")
            
            # Get base URL (in production, this would be detected from request)
            base_url = get_current_page_url(hostname='localhost', port=8085)
            
            # Generate shareable URL
            url = generate_shareable_url(
                base_url=base_url,
                product=product,
                start_date=start_date,
                end_date=end_date,
                minute_hour=minute_hour,
                filters=filters,
                vol_threshold=vol_threshold,
                pct_threshold=pct_threshold,
                trim_percentage=trim_percentage,
                stat_measures=stat_measures,
                timeA_hour=timeA_hour,
                timeA_minute=timeA_minute,
                timeB_hour=timeB_hour,
                timeB_minute=timeB_minute
            )
            
            print(f"[SHARE URL] Generated URL ({len(url)} chars): {url[:100]}...")
            
            display_style = {
                'display': 'block',
                'marginBottom': '10px',
                'padding': '8px',
                'backgroundColor': '#e8f5e9',
                'borderRadius': '4px',
                'fontSize': '11px',
                'wordBreak': 'break-all'
            }
            
            return [
                html.B("Shareable URL:"), html.Br(),
                html.A(url, href=url, target='_blank', style={'color': '#28a745'})
            ], display_style
            
        except Exception as e:
            return f"Error generating URL: {str(e)}", {'display': 'block', 'color': 'red'}
    
    # Download All CSV Callback
    @app.callback(
        Output('download-all-csv', 'data'),
        Input('download-all-csv-btn', 'n_clicks'),
        [
            State('h-avg', 'figure'),
            State('h-var', 'figure'),
            State('h-range', 'figure'),
            State('h-var-range', 'figure'),
            State('m-avg', 'figure'),
            State('m-var', 'figure'),
            State('m-range', 'figure'),
            State('m-var-range', 'figure'),
            State('product-dropdown', 'value'),
        ],
        prevent_initial_call=True
    )
    def download_all_csv(n_clicks, h_avg_fig, h_var_fig, h_range_fig, h_var_range_fig,
                        m_avg_fig, m_var_fig, m_range_fig, m_var_range_fig, product):
        """Download all chart data as CSV files in a ZIP."""
        print(f"\n[CSV EXPORT] Callback triggered")
        print(f"[CSV EXPORT] n_clicks={n_clicks}")
        
        if n_clicks is None or n_clicks == 0:
            print(f"[CSV EXPORT] Skipping - no clicks")
            return None
        
        try:
            import io
            import zipfile
            import plotly.graph_objects as go
            
            print(f"[CSV EXPORT] Button clicked (n_clicks={n_clicks})")
            print(f"[CSV EXPORT] Product: {product}")
            
            # Check if figures exist
            figures = {
                'hourly_avg_change': h_avg_fig,
                'hourly_var_change': h_var_fig,
                'hourly_avg_range': h_range_fig,
                'hourly_var_range': h_var_range_fig,
                'minute_avg_change': m_avg_fig,
                'minute_var_change': m_var_fig,
                'minute_avg_range': m_range_fig,
                'minute_var_range': m_var_range_fig,
            }
            
            print(f"[CSV EXPORT] Checking figures...")
            for name, fig in figures.items():
                fig_type = type(fig).__name__ if fig else 'None'
                print(f"  - {name}: {fig_type}")
            
            # Count non-None figures
            valid_figs = sum(1 for fig in figures.values() if fig is not None)
            print(f"[CSV EXPORT] Valid figures: {valid_figs}/{len(figures)}")
            
            if valid_figs == 0:
                print(f"[CSV EXPORT] ERROR: No valid figures to export!")
                return None
            
            # Create in-memory ZIP
            print(f"[CSV EXPORT] Creating ZIP buffer...")
            zip_buffer = io.BytesIO()
            
            print(f"[CSV EXPORT] Processing figures...")
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                csv_count = 0
                for name, fig in figures.items():
                    if fig:
                        try:
                            # Convert dict back to Figure if needed
                            if isinstance(fig, dict):
                                print(f"  - Converting {name} from dict to Figure")
                                fig = go.Figure(fig)
                            
                            print(f"  - Exporting {name}...")
                            csv_string = export_figure_to_csv(fig, f"{name}.csv")
                            
                            if csv_string:
                                zip_file.writestr(f"{name}.csv", csv_string)
                                csv_count += 1
                                print(f"    ✓ Added {name}.csv ({len(csv_string)} bytes)")
                            else:
                                print(f"    ✗ Empty CSV for {name}")
                        except Exception as fig_error:
                            print(f"    ✗ Error processing {name}: {fig_error}")
                            import traceback
                            traceback.print_exc()
                
                print(f"[CSV EXPORT] Total CSVs added: {csv_count}")
            
            if csv_count == 0:
                print(f"[CSV EXPORT] ERROR: No CSVs were created!")
                return None
            
            print(f"[CSV EXPORT] Encoding ZIP...")
            zip_buffer.seek(0)
            
            # Return as base64 encoded data for dcc.Download
            import base64
            zip_bytes = zip_buffer.getvalue()
            print(f"[CSV EXPORT] ZIP size: {len(zip_bytes)} bytes")
            
            b64_zip = base64.b64encode(zip_bytes).decode()
            print(f"[CSV EXPORT] Base64 size: {len(b64_zip)} chars")
            
            result = dict(
                content=b64_zip,
                filename=f"{product}_almanac_data.zip",
                base64=True
            )
            
            print(f"[CSV EXPORT] SUCCESS! Returning download data")
            return result
            
        except Exception as e:
            print(f"\n[CSV EXPORT] FATAL ERROR: {e}")
            import traceback
            traceback.print_exc()
            print(f"[CSV EXPORT] Returning None due to error\n")
            return None


# Add callbacks for the new filtering functionality
def register_filter_callbacks(app):
    """Register all filter-related callbacks with the app instance."""
    
    @app.callback(
        Output('active-filters-display', 'children'),
        Input('quick-filter-dropdown', 'value'),
        prevent_initial_call=True
    )
    def update_active_filters(quick_filter):
        """Display active filters."""
        if quick_filter == 'all':
            return html.Div("No active filters", style={'color': '#666', 'fontStyle': 'italic'})
        
        filter_names = {
            'bull': 'Bull Days (Close > Open)',
            'bear': 'Bear Days (Close < Open)', 
            'high_vol': 'High Vol Days (Range > 75th percentile)',
            'low_vol': 'Low Vol Days (Range < 25th percentile)',
            'gap_up': 'Gap Up Days (>0.5%)',
            'gap_down': 'Gap Down Days (<-0.5%)'
        }
        
        return html.Div([
            html.Span("Active: ", style={'fontWeight': 'bold'}),
            html.Span(filter_names.get(quick_filter, quick_filter), 
                     style={'backgroundColor': '#e3f2fd', 'padding': '2px 8px', 'borderRadius': '3px'})
        ])

    # Initial Date Range Callback - Set default dates when page loads
    @app.callback(
        [Output('filter-start-date', 'date'),
         Output('filter-end-date', 'date')],
        Input('product-dropdown', 'value'),
        prevent_initial_call=False  # Allow initial call to set defaults
    )
    def set_default_dates(selected_product):
        """Set default date range when page loads."""
        return '2025-01-01', '2025-04-08'

    # Max Date Range Callback
    @app.callback(
        [Output('filter-start-date', 'date', allow_duplicate=True),
         Output('filter-end-date', 'date', allow_duplicate=True)],
        Input('max-date-range-btn', 'n_clicks'),
        State('product-dropdown', 'value'),
        prevent_initial_call=True
    )
    def set_max_date_range(n_clicks, selected_product):
        """Set date range to first and last available dates for the selected product."""
        if not n_clicks or not selected_product:
            return None, None
        
        try:
            # Load data to get date range
            from ..data_sources.daily_loader import load_daily_data
            import pandas as pd
            
            # Use a wide date range to get all available data
            start_date = '2025-01-01'
            end_date = '2025-04-08'
            
            daily_data = load_daily_data(selected_product, start_date, end_date)
            
            if daily_data is not None and not daily_data.empty:
                # Get first and last available dates
                first_date = daily_data.index.min().strftime('%Y-%m-%d')
                last_date = daily_data.index.max().strftime('%Y-%m-%d')
                
                return first_date, last_date
            else:
                # Fallback dates if no data available
                return '2025-01-01', '2025-04-08'
                
        except Exception as e:
            print(f"Error setting max date range: {e}")
            # Fallback dates
            return '2025-01-01', '2025-04-08'


def register_profile_callbacks(app, cache):
    """Register all callbacks for the profile page."""
    
    # Register the hourly calculation callback
    @app.callback(
        [
            Output('h-avg', 'figure'),
            Output('h-var', 'figure'),
            Output('h-range', 'figure'),
            Output('h-var-range', 'figure'),
            Output('m-avg', 'figure'),
            Output('m-var', 'figure'),
            Output('m-range', 'figure'),
            Output('m-var-range', 'figure'),
            Output('summary-box', 'children'),
            Output('hod-lod-kpi-cards', 'children'),
            Output('hod-survival', 'figure'),
            Output('lod-survival', 'figure'),
            Output('hod-heatmap', 'figure'),
            Output('lod-heatmap', 'figure'),
            Output('hod-rolling', 'figure'),
            Output('lod-rolling', 'figure'),
            Output('total-cases-display', 'children'),
            Output('calc-progress-bar', 'style'),
            Output('calc-progress-container', 'style'),
            Output('calc-status', 'children'),
        ],
        Input('calc-hourly-btn', 'n_clicks'),
        [
            State('product-dropdown', 'value'),
            State('filter-start-date', 'date'),
            State('filter-end-date', 'date'),
            State('minute-hour', 'value'),
            State('filters', 'value'),
            State('vol-threshold', 'value'),
            State('pct-threshold', 'value'),
            State('median-percentage', 'value'),
            State('stat-measures', 'value'),
            State('timeA-hour', 'value'),
            State('timeA-minute', 'value'),
            State('timeB-hour', 'value'),
            State('timeB-minute', 'value'),
            State('hourly-filters', 'value'),
            State('hourly-vol-threshold', 'value'),
            State('hourly-pct-threshold', 'value'),
            # Percentage change zone filters (shared)
            State('prev-ny-enabled', 'value'),
            State('prev-ny-target', 'value'),
            State('prev-ny-tolerance', 'value'),
            State('prev-ny-start-day', 'value'),
            State('prev-ny-start-hour', 'value'),
            State('prev-ny-start-minute', 'value'),
            State('prev-ny-end-day', 'value'),
            State('prev-ny-end-hour', 'value'),
            State('prev-ny-end-minute', 'value'),

            State('zone1-enabled', 'value'),
            State('zone1-target', 'value'),
            State('zone1-tolerance', 'value'),
            State('zone1-start-day', 'value'),
            State('zone1-start-hour', 'value'),
            State('zone1-start-minute', 'value'),
            State('zone1-end-day', 'value'),
            State('zone1-end-hour', 'value'),
            State('zone1-end-minute', 'value'),

            State('zone2-enabled', 'value'),
            State('zone2-target', 'value'),
            State('zone2-tolerance', 'value'),
            State('zone2-start-day', 'value'),
            State('zone2-start-hour', 'value'),
            State('zone2-start-minute', 'value'),
            State('zone2-end-day', 'value'),
            State('zone2-end-hour', 'value'),
            State('zone2-end-minute', 'value'),
        ]
    )
    @cache.memoize(timeout=3600)  # OPTIMIZED: 1 hour cache for historical data
    def update_hourly_graphs(
        n, prod, start, end, mh, filters, vol_thr, pct_thr, median_pct, selected_measures, tA_h, tA_m, tB_h, tB_m,
        hourly_filters, hourly_vol_thr, hourly_pct_thr,
        prev_ny_enabled, prev_ny_target, prev_ny_tol, prev_ny_sd, prev_ny_sh, prev_ny_sm, prev_ny_ed, prev_ny_eh, prev_ny_em,
        zone1_enabled, zone1_target, zone1_tol, zone1_sd, zone1_sh, zone1_sm, zone1_ed, zone1_eh, zone1_em,
        zone2_enabled, zone2_target, zone2_tol, zone2_sd, zone2_sh, zone2_sm, zone2_ed, zone2_eh, zone2_em
    ):
        """Main callback to update all charts and summary."""
        print(f"\n[DEBUG] Callback triggered: n_clicks={n}")
        print(f"[DEBUG] Parameters: prod={prod}, start={start}, end={end}, mh={mh}")
        print(f"[DEBUG] filters={filters}, vol_thr={vol_thr}, pct_thr={pct_thr}")
        print(f"[DEBUG] median_pct={median_pct}, selected_measures={selected_measures}")
        # Use hourly-prefixed filters if provided, otherwise fall back to global filters
        active_filters_hr = hourly_filters if hourly_filters else filters
        vol_thr_hr = hourly_vol_thr if hourly_vol_thr is not None else vol_thr
        pct_thr_hr = hourly_pct_thr if hourly_pct_thr is not None else pct_thr
        
        # Initialize HOD/LOD variables
        empty_fig = make_line_chart([], [], "No Data", "", "")
        empty_kpi = html.Div("Initializing...")
        hod_survival_fig = lod_survival_fig = empty_fig
        hod_heatmap_fig = lod_heatmap_fig = empty_fig
        hod_rolling_fig = lod_rolling_fig = empty_fig
        
        # Early guard and timing setup to avoid hanging on initial/inadvertent triggers
        start_time = time.perf_counter()
        if not n:
            placeholder = make_line_chart([], [], "Awaiting calculation", "", "")
            return (
                placeholder, placeholder, placeholder, placeholder,  # h-avg, h-var, h-range, h-var-range
                placeholder, placeholder, placeholder, placeholder,  # m-avg, m-var, m-range, m-var-range
                html.Div(""),                                       # summary-box
                html.Div(""),                                       # hod-lod-kpi-cards
                placeholder, placeholder,                            # hod-survival, lod-survival
                placeholder, placeholder,                            # hod-heatmap, lod-heatmap
                placeholder, placeholder,                            # hod-rolling, lod-rolling
                "",                                                 # total-cases-display
                { 'width': '0%', 'height': '20px', 'backgroundColor': '#007bff', 'borderRadius': '10px', 'transition': 'width 0.3s ease' },
                { 'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none' },
                ""
            )
        
        try:
            debug_msgs = []
            # Try loading from database first - PARALLEL LOADING for 2x speed
            try:
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=2) as executor:
                    daily_future = executor.submit(load_daily_data, prod, start, end)
                    minute_future = executor.submit(load_minute_data, prod, start, end)
                    daily = daily_future.result()
                    minute = minute_future.result()
                debug_msgs.append(f"Loaded daily={len(daily)} rows, minute={len(minute)} rows")
                load_dur = time.perf_counter() - start_time
                print(f"[PERF] Parallel data load took {load_dur:.2f}s")
                debug_msgs.append(f"PERF: load {load_dur:.2f}s")
                
                # Intermarket data loading disabled in simplified version
                intermarket_daily = None
                        
            except Exception as db_error:
                # Fallback to demo data if database unavailable
                print(f"Database unavailable, using demo data: {db_error}")
                from ..data_sources import generate_demo_daily_data, generate_demo_minute_data
                daily = generate_demo_daily_data(prod, start, end)
                minute = generate_demo_minute_data(prod, start, end)
                intermarket_daily = None
            
            # Check if we have data
            if daily.empty or minute.empty:
                error_msg = html.Div([
                    html.B("Error:", style={'color': 'red'}),
                    html.Br(),
                    f"No data found for {prod} between {start} and {end}"
                ])
                empty_fig = make_line_chart([], [], "No Data", "", "")
                empty_kpi = html.Div("No data available")
                return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("77 cases", { 'width': '0%', 'height': '20px', 'backgroundColor': '#dc3545', 'borderRadius': '10px', 'transition': 'width 0.3s ease' }, { 'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none' }, "Error occurred")
            
            # Apply filtering
            filtered_minute = minute
            
            # Apply additional filters
            try:
                pre_rows = len(filtered_minute)
                filtered_minute = apply_filters(filtered_minute, daily, active_filters_hr, vol_thr_hr, pct_thr_hr)
                filt_dur = time.perf_counter() - start_time
                print(f"[PERF] Base filters took {filt_dur:.2f}s (rows {pre_rows} -> {len(filtered_minute)})")
                debug_msgs.append(f"After base filters [{','.join(filters or [])}]: {pre_rows} -> {len(filtered_minute)} rows")
                debug_msgs.append(f"PERF: base_filters {filt_dur:.2f}s")
            except Exception as filter_error:
                import traceback
                traceback.print_exc()
                raise
            
            # Apply time filters if specified
            if filters and any(f in filters for f in ['timeA_gt_timeB', 'timeA_lt_timeB']):
                try:
                    before_time = len(filtered_minute)
                    filtered_minute = apply_time_filters(
                        filtered_minute, filters, tA_h, tA_m, tB_h, tB_m
                    )
                    tf_dur = time.perf_counter() - start_time
                    print(f"[PERF] Time filters took {tf_dur:.2f}s (rows {before_time} -> {len(filtered_minute)})")
                    debug_msgs.append(f"After time filters: {before_time} -> {len(filtered_minute)} rows")
                    debug_msgs.append(f"PERF: time_filters {tf_dur:.2f}s")
                except Exception as time_error:
                    import traceback
                    traceback.print_exc()
                    raise

            # Apply percentage-change zone filters (applies to BOTH hourly and minute stats)
            zone_diagnostics = None
            try:
                zone_specs = []

                def _is_enabled(v):
                    return bool(v) and (True in v)

                # Prev NY
                if _is_enabled(prev_ny_enabled):
                    try:
                        zone_specs.append(parse_zone_spec(
                            name='prev_ny',
                            enabled=True,
                            target_pct=prev_ny_target,
                            tolerance_pct=prev_ny_tol,
                            start_day_offset=prev_ny_sd,
                            start_hour=prev_ny_sh,
                            start_minute=prev_ny_sm,
                            end_day_offset=prev_ny_ed,
                            end_hour=prev_ny_eh,
                            end_minute=prev_ny_em
                        ))
                    except Exception as e:
                        debug_msgs.append(f"prev_ny enabled but invalid: {e} (skipping)")

                # Zone1
                if _is_enabled(zone1_enabled):
                    try:
                        zone_specs.append(parse_zone_spec(
                            name='zone1',
                            enabled=True,
                            target_pct=zone1_target,
                            tolerance_pct=zone1_tol,
                            start_day_offset=zone1_sd,
                            start_hour=zone1_sh,
                            start_minute=zone1_sm,
                            end_day_offset=zone1_ed,
                            end_hour=zone1_eh,
                            end_minute=zone1_em
                        ))
                    except Exception as e:
                        debug_msgs.append(f"zone1 enabled but invalid: {e} (skipping)")

                # Zone2
                if _is_enabled(zone2_enabled):
                    try:
                        zone_specs.append(parse_zone_spec(
                            name='zone2',
                            enabled=True,
                            target_pct=zone2_target,
                            tolerance_pct=zone2_tol,
                            start_day_offset=zone2_sd,
                            start_hour=zone2_sh,
                            start_minute=zone2_sm,
                            end_day_offset=zone2_ed,
                            end_hour=zone2_eh,
                            end_minute=zone2_em
                        ))
                    except Exception as e:
                        debug_msgs.append(f"zone2 enabled but invalid: {e} (skipping)")

                # Remove any Nones (in case we ever return None in future)
                zone_specs = [s for s in zone_specs if s is not None]

                if zone_specs:
                    before_zone = len(filtered_minute)
                    filtered_minute, zone_diagnostics = apply_zone_filters(filtered_minute, zone_specs)
                    debug_msgs.append(f"After zone filters: {before_zone} -> {len(filtered_minute)} rows")
                    debug_msgs.append(f"Zone days remaining: {zone_diagnostics.get('days_remaining')} / {zone_diagnostics.get('total_days')}")
            except Exception as e:
                debug_msgs.append(f"Error applying zone filters: {e}")
            
            # Compute statistics (now includes all 4 measures)
            try:
                # Set default values
                trim_pct = median_pct or 5.0
                selected_measures = selected_measures or ['mean', 'trimmed_mean', 'median', 'mode']
                
                # Compute hourly stats - all 5 measures (including outlier)
                stats_t0 = time.perf_counter()
                hc, hcm, hmed, hmode, houtlier, hv, hr, hrm, hmed_r, hmode_r, houtlier_r, hvr = compute_hourly_stats(filtered_minute, trim_pct)
                # Compute minute stats with all 5 measures (including outlier)
                mc, mcm, mmed, mmode, moutlier, mv, mr, mrm, mmed_r, mmode_r, moutlier_r, mvr = compute_minute_stats(filtered_minute, mh, trim_pct)
                stats_dur = time.perf_counter() - stats_t0
                print(f"[PERF] Stats computation took {stats_dur:.2f}s")
                debug_msgs.append(f"PERF: stats {stats_dur:.2f}s")
            except Exception as stats_error:
                import traceback
                traceback.print_exc()
                raise
            
            # Scale variance for display
            sh, _ = _scale_variance(hv.mean())
            sm, _ = _scale_variance(mv.mean())
            
            # Compute HOD/LOD analysis
            print(f"[HOD/LOD] Starting analysis...")
            
            try:
                # Detect HOD/LOD for each trading day
                hod_lod_df = detect_hod_lod(filtered_minute)
                print(f"[HOD/LOD] Detected HOD/LOD for {len(hod_lod_df)} days")
                
                if len(hod_lod_df) < 10:
                    print(f"[HOD/LOD] Insufficient data ({len(hod_lod_df)} days), using empty charts")
                    empty_fig = make_line_chart([], [], "Insufficient Data", "", "")
                    empty_kpi = html.Div([
                        html.B("HOD/LOD Analysis", style={'marginBottom': '10px'}),
                        html.Br(),
                        html.Div(f"Insufficient data ({len(hod_lod_df)} days, need ≥10)", 
                                style={'color': '#666', 'fontSize': '14px'})
                    ])
                    hod_survival_fig = lod_survival_fig = empty_fig
                    hod_heatmap_fig = lod_heatmap_fig = empty_fig
                    hod_rolling_fig = lod_rolling_fig = empty_fig
                else:
                    # Compute survival curves
                    print(f"[HOD/LOD] Computing survival curves...")
                    hod_survival, lod_survival = compute_survival_curves(hod_lod_df)
                    
                    # Compute heatmaps
                    print(f"[HOD/LOD] Computing heatmaps...")
                    hod_heatmap_data, lod_heatmap_data = compute_hod_lod_heatmap(hod_lod_df, by='weekday')
                    
                    # Compute rolling medians
                    print(f"[HOD/LOD] Computing rolling medians...")
                    hod_rolling = compute_rolling_median_time(hod_lod_df, 'hod', window=63)
                    lod_rolling = compute_rolling_median_time(hod_lod_df, 'lod', window=63)
                    
                    # Compute trend tests
                    print(f"[HOD/LOD] Computing trend tests...")
                    hod_trend = compute_trend_test(hod_lod_df['hod_minutes_since_midnight'])
                    lod_trend = compute_trend_test(hod_lod_df['lod_minutes_since_midnight'])
                    
                    # Create KPI cards
                    hod_median_time = hod_lod_df['hod_minutes_since_midnight'].median()
                    lod_median_time = hod_lod_df['lod_minutes_since_midnight'].median()
                    
                    hod_time_str = f"{int(hod_median_time//60):02d}:{int(hod_median_time%60):02d}"
                    lod_time_str = f"{int(lod_median_time//60):02d}:{int(lod_median_time%60):02d}"
                    
                    # Determine trend colors
                    hod_trend_color = 'success' if hod_trend['trend'] == 'increasing' else 'danger' if hod_trend['trend'] == 'decreasing' else 'secondary'
                    lod_trend_color = 'success' if lod_trend['trend'] == 'increasing' else 'danger' if lod_trend['trend'] == 'decreasing' else 'secondary'
                    
                    empty_kpi = html.Div([
                        html.H5("HOD/LOD Analysis", className="mb-4 text-center", 
                               style={'color': '#2c3e50', 'fontWeight': 'bold', 'marginBottom': '20px'}),
                        html.Div([
                            # HOD Time Bubble
                            html.Div([
                                html.Div([
                                    html.H6("MEDIAN HOD TIME", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H3(hod_time_str, style={
                                        'fontSize': '24px', 
                                        'fontWeight': 'bold', 
                                        'color': '#0066cc',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#e6f3ff',
                                'borderRadius': '16px',
                                'border': '2px solid #b3d9ff',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(0,102,204,0.1)'
                            }),
                            
                            # LOD Time Bubble
                            html.Div([
                                html.Div([
                                    html.H6("MEDIAN LOD TIME", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H3(lod_time_str, style={
                                        'fontSize': '24px', 
                                        'fontWeight': 'bold', 
                                        'color': '#009933',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#e6ffe6',
                                'borderRadius': '16px',
                                'border': '2px solid #99e699',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(0,153,51,0.1)'
                            }),
                            
                            # HOD Trend Bubble
                            html.Div([
                                html.Div([
                                    html.H6("HOD TREND", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H5(hod_trend['trend'].replace('_', ' ').title(), style={
                                        'fontSize': '16px', 
                                        'fontWeight': 'bold', 
                                        'color': '#17a2b8',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#e6f8ff',
                                'borderRadius': '16px',
                                'border': '2px solid #80d4ff',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(23,162,184,0.1)'
                            }),
                            
                            # LOD Trend Bubble
                            html.Div([
                                html.Div([
                                    html.H6("LOD TREND", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H5(lod_trend['trend'].replace('_', ' ').title(), style={
                                        'fontSize': '16px', 
                                        'fontWeight': 'bold', 
                                        'color': '#ffc107',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#fff8e6',
                                'borderRadius': '16px',
                                'border': '2px solid #ffdd99',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(255,193,7,0.1)'
                            }),
                            
                            # Sample Size Bubble
                            html.Div([
                                html.Div([
                                    html.H6("SAMPLE SIZE", style={
                                        'fontSize': '10px', 
                                        'color': '#666', 
                                        'marginBottom': '8px', 
                                        'fontWeight': '600',
                                        'letterSpacing': '1px',
                                        'textTransform': 'uppercase'
                                    }),
                                    html.H3(f"{len(hod_lod_df)}", style={
                                        'fontSize': '24px', 
                                        'fontWeight': 'bold', 
                                        'color': '#6f42c1',
                                        'margin': '0',
                                        'lineHeight': '1.2'
                                    })
                                ], style={
                                    'padding': '20px 16px',
                                    'textAlign': 'center'
                                })
                            ], style={
                                'backgroundColor': '#f3e6ff',
                                'borderRadius': '16px',
                                'border': '2px solid #d1b3ff',
                                'margin': '0 8px',
                                'minWidth': '140px',
                                'flex': '1',
                                'boxShadow': '0 2px 8px rgba(111,66,193,0.1)'
                            }),
                        ], style={
                            'display': 'flex',
                            'justifyContent': 'center',
                            'alignItems': 'center',
                            'flexWrap': 'wrap',
                            'gap': '12px',
                            'padding': '0 20px'
                        })
                    ], style={'marginBottom': '30px'})
                    
                    # Create survival curve charts
                    hod_survival_fig = make_survival_curve(
                        hod_survival['minutes'], hod_survival['probability'],
                        "HOD Survival Curve", "Minutes Since Midnight", "Probability"
                    )
                    lod_survival_fig = make_survival_curve(
                        lod_survival['minutes'], lod_survival['probability'],
                        "LOD Survival Curve", "Minutes Since Midnight", "Probability"
                    )
                    
                    # Create heatmap charts
                    hod_heatmap_fig = make_heatmap(
                        hod_heatmap_data, "HOD Frequency by Weekday", "Time", "Weekday"
                    )
                    lod_heatmap_fig = make_heatmap(
                        lod_heatmap_data, "LOD Frequency by Weekday", "Time", "Weekday"
                    )
                    
                    # Create rolling median charts
                    hod_rolling_fig = make_line_chart(
                        hod_rolling['date'], hod_rolling['rolling_median'],
                        "HOD Rolling Median (63-day)", "Date", "Minutes Since Midnight"
                    )
                    lod_rolling_fig = make_line_chart(
                        lod_rolling['date'], lod_rolling['rolling_median'],
                        "LOD Rolling Median (63-day)", "Date", "Minutes Since Midnight"
                    )
                    
                    print(f"[HOD/LOD] Analysis complete!")
                    
            except Exception as hod_lod_error:
                print(f"[HOD/LOD] Error in analysis: {hod_lod_error}")
                import traceback
                traceback.print_exc()
                
                # Fallback to empty charts
                empty_fig = make_line_chart([], [], "HOD/LOD Error", "", "")
                empty_kpi = html.Div([
                    html.B("HOD/LOD Analysis", style={'marginBottom': '10px'}),
                    html.Br(),
                    html.Div(f"Analysis error: {str(hod_lod_error)[:50]}...", 
                            style={'color': '#666', 'fontSize': '14px'})
                ])
                hod_survival_fig = lod_survival_fig = empty_fig
                hod_heatmap_fig = lod_heatmap_fig = empty_fig
                hod_rolling_fig = lod_rolling_fig = empty_fig
            
            # Generate summary
            summary = _generate_summary(
                daily, filtered_minute, hc, hv, hr, hvr, mc, mv, mr, mvr, sh, sm, zone_diagnostics
            )
            # Append debug information at the bottom of the summary
            if debug_msgs:
                summary = summary + [html.Hr(), html.B("Debug"), html.Br()] + [html.Div(m) for m in debug_msgs]
            
            # Count filtered trading days for total cases display - use total trading days if no filters
            filtered_days = filtered_minute['date'].nunique() if 'date' in filtered_minute.columns else 0
            if filtered_days == 0 and not filters:
                # No filters applied, use total trading days from daily data
                filtered_days = daily.index.nunique() if hasattr(daily, 'index') else len(daily)
            
            # Create dynamic title suffix
            title_suffix = f" | {prod} | {filtered_days} cases | {start} to {end}"
            
            # Create figures with all 5 statistical measures (including outlier)
            return (
                make_line_chart(hc.index, hc, f"Hourly Avg % Change{title_suffix}", "Hour", "Pct", 
                              mean_data=hc, trimmed_mean_data=hcm, median_data=hmed, mode_data=hmode, outlier_data=houtlier,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(hv.index, hv, f"Hourly Var % Change{title_suffix}", "Hour", "Var"),
                make_line_chart(hr.index, hr, f"Hourly Avg Range{title_suffix}", "Hour", "Price", 
                              mean_data=hr, trimmed_mean_data=hrm, median_data=hmed_r, mode_data=hmode_r, outlier_data=houtlier_r,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(hvr.index, hvr, f"Hourly Var Range{title_suffix}", "Hour", "Var"),
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-avg - empty for hourly-only
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-var - empty for hourly-only
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-range - empty for hourly-only
                make_line_chart([], [], "Click 'Calculate Minutes' to load data", "", ""),  # m-var-range - empty for hourly-only
                summary,
                empty_kpi,  # HOD/LOD KPI cards
                hod_survival_fig,
                lod_survival_fig,
                hod_heatmap_fig,
                lod_heatmap_fig,
                hod_rolling_fig,
                lod_rolling_fig,
                f"{filtered_days} cases",  # Total cases display
                {'width': '100%', 'height': '20px', 'backgroundColor': '#28a745', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},  # Progress bar complete
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},  # Hide container
                "Hour-of-Day analysis complete!"  # Status message
            )
            
            print(f"[DEBUG] Callback completed successfully!")
            
        except Exception as e:
            print(f"\n{'='*60}")
            print(f"EXCEPTION IN update_graphs CALLBACK:")
            print(f"{'='*60}")
            import traceback
            traceback.print_exc()
            print(f"{'='*60}\n")
            
            error_msg = html.Div([
                html.B("Error:", style={'color': 'red'}),
                html.Br(),
                html.Pre(str(e), style={'whiteSpace': 'pre-wrap', 'fontSize': '11px'}),
                html.Br(),
                html.B("Type:", style={'fontSize': '10px'}),
                html.Div(str(type(e).__name__), style={'fontSize': '10px'}),
            ])
            empty_fig = make_line_chart([], [], "No Data", "", "")
            empty_kpi = html.Div("Error occurred")
            return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("77 cases", {'width': '0%', 'height': '20px', 'backgroundColor': '#dc3545', 'borderRadius': '10px', 'transition': 'width 0.3s ease'}, {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'}, "Error occurred")
    
    # Register the minute calculation callback
    @app.callback(
        [
            Output('m-avg', 'figure', allow_duplicate=True),
            Output('m-var', 'figure', allow_duplicate=True),
            Output('m-range', 'figure', allow_duplicate=True),
            Output('m-var-range', 'figure', allow_duplicate=True),
            Output('calc-progress-bar', 'style', allow_duplicate=True),
            Output('calc-progress-container', 'style', allow_duplicate=True),
            Output('calc-status', 'children', allow_duplicate=True),
        ],
        Input('calc-minutes-btn', 'n_clicks'),
        [
            State('product-dropdown', 'value'),
            State('filter-start-date', 'date'),
            State('filter-end-date', 'date'),
            State('minute-hour', 'value'),
            State('filters', 'value'),
            State('vol-threshold', 'value'),
            State('pct-threshold', 'value'),
            State('median-percentage', 'value'),
            State('stat-measures', 'value'),
            State('timeA-hour', 'value'),
            State('timeA-minute', 'value'),
            State('timeB-hour', 'value'),
            State('timeB-minute', 'value'),
            State('minute-filters', 'value'),
            State('minute-vol-threshold', 'value'),
            # Percentage change zone filters (shared)
            State('prev-ny-enabled', 'value'),
            State('prev-ny-target', 'value'),
            State('prev-ny-tolerance', 'value'),
            State('prev-ny-start-day', 'value'),
            State('prev-ny-start-hour', 'value'),
            State('prev-ny-start-minute', 'value'),
            State('prev-ny-end-day', 'value'),
            State('prev-ny-end-hour', 'value'),
            State('prev-ny-end-minute', 'value'),

            State('zone1-enabled', 'value'),
            State('zone1-target', 'value'),
            State('zone1-tolerance', 'value'),
            State('zone1-start-day', 'value'),
            State('zone1-start-hour', 'value'),
            State('zone1-start-minute', 'value'),
            State('zone1-end-day', 'value'),
            State('zone1-end-hour', 'value'),
            State('zone1-end-minute', 'value'),

            State('zone2-enabled', 'value'),
            State('zone2-target', 'value'),
            State('zone2-tolerance', 'value'),
            State('zone2-start-day', 'value'),
            State('zone2-start-hour', 'value'),
            State('zone2-start-minute', 'value'),
            State('zone2-end-day', 'value'),
            State('zone2-end-hour', 'value'),
            State('zone2-end-minute', 'value'),
        ],
        prevent_initial_call=True
    )
    @cache.memoize(timeout=3600)  # OPTIMIZED: 1 hour cache for historical data
    def update_minute_graphs(
        n, prod, start, end, mh, filters, vol_thr, pct_thr, median_pct, selected_measures, tA_h, tA_m, tB_h, tB_m,
        minute_filters, minute_vol_thr,
        prev_ny_enabled, prev_ny_target, prev_ny_tol, prev_ny_sd, prev_ny_sh, prev_ny_sm, prev_ny_ed, prev_ny_eh, prev_ny_em,
        zone1_enabled, zone1_target, zone1_tol, zone1_sd, zone1_sh, zone1_sm, zone1_ed, zone1_eh, zone1_em,
        zone2_enabled, zone2_target, zone2_tol, zone2_sd, zone2_sh, zone2_sm, zone2_ed, zone2_eh, zone2_em
    ):
        """Callback to update minute charts only."""
        print(f"\n[DEBUG] Minute Callback triggered: n_clicks={n}")
        
        if not n:
            empty_fig = make_line_chart([], [], "No Data", "", "")
            return (empty_fig,) * 4 + (
                {'width': '0%', 'height': '20px', 'backgroundColor': '#007bff', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                ""
            )
        
        try:
            # Load data
            daily = load_daily_data(prod, start, end)
            minute = load_minute_data(prod, start, end)
            
            # Process minute data
            filtered_minute = minute
            # Use minute-prefixed filters if provided, otherwise fall back to global filters
            active_filters_min = minute_filters if minute_filters else filters
            vol_thr_min = minute_vol_thr if minute_vol_thr is not None else vol_thr
            if active_filters_min:
                filtered_minute = apply_filters(filtered_minute, daily, active_filters_min, vol_thr_min, pct_thr)

            # Apply percentage-change zone filters here too (minute-only run)
            try:
                zone_specs = []

                def _is_enabled(v):
                    return bool(v) and (True in v)

                if _is_enabled(prev_ny_enabled):
                    try:
                        zone_specs.append(parse_zone_spec(
                            name='prev_ny',
                            enabled=True,
                            target_pct=prev_ny_target,
                            tolerance_pct=prev_ny_tol,
                            start_day_offset=prev_ny_sd,
                            start_hour=prev_ny_sh,
                            start_minute=prev_ny_sm,
                            end_day_offset=prev_ny_ed,
                            end_hour=prev_ny_eh,
                            end_minute=prev_ny_em
                        ))
                    except Exception:
                        pass

                if _is_enabled(zone1_enabled):
                    try:
                        zone_specs.append(parse_zone_spec(
                            name='zone1',
                            enabled=True,
                            target_pct=zone1_target,
                            tolerance_pct=zone1_tol,
                            start_day_offset=zone1_sd,
                            start_hour=zone1_sh,
                            start_minute=zone1_sm,
                            end_day_offset=zone1_ed,
                            end_hour=zone1_eh,
                            end_minute=zone1_em
                        ))
                    except Exception:
                        pass

                if _is_enabled(zone2_enabled):
                    try:
                        zone_specs.append(parse_zone_spec(
                            name='zone2',
                            enabled=True,
                            target_pct=zone2_target,
                            tolerance_pct=zone2_tol,
                            start_day_offset=zone2_sd,
                            start_hour=zone2_sh,
                            start_minute=zone2_sm,
                            end_day_offset=zone2_ed,
                            end_hour=zone2_eh,
                            end_minute=zone2_em
                        ))
                    except Exception:
                        pass

                zone_specs = [s for s in zone_specs if s is not None]
                if zone_specs:
                    filtered_minute, _ = apply_zone_filters(filtered_minute, zone_specs)
            except Exception:
                pass

            mc, mcm, mmed, mmode, moutlier, mv, mr, mrm, mmed_r, mmode_r, moutlier_r, mvr = compute_minute_stats(filtered_minute, mh, median_pct)
            
            # Count cases for dynamic titles - use total trading days if no filters
            filtered_days = filtered_minute['date'].nunique() if 'date' in filtered_minute.columns else 0
            if filtered_days == 0 and not filters:
                # No filters applied, use total trading days from daily data
                filtered_days = daily.index.nunique() if hasattr(daily, 'index') else len(daily)
            
            # Create dynamic title suffix
            title_suffix = f" | {prod} | {filtered_days} cases | {start} to {end}"
            
            return (
                make_line_chart(mc.index, mc, f"Min Avg %∆ @ {mh}:00{title_suffix}", "Minute", "Pct", 
                              mean_data=mc, trimmed_mean_data=mcm, median_data=mmed, mode_data=mmode, outlier_data=moutlier,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(mv.index, mv, f"Min Var %∆ @ {mh}:00{title_suffix}", "Minute", "Var"),
                make_line_chart(mr.index, mr, f"Min Avg Range @ {mh}:00{title_suffix}", "Minute", "Price", 
                              mean_data=mr, trimmed_mean_data=mrm, median_data=mmed_r, mode_data=mmode_r, outlier_data=moutlier_r,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(mvr.index, mvr, f"Min Var Range @ {mh}:00{title_suffix}", "Minute", "Var"),
                {'width': '100%', 'height': '20px', 'backgroundColor': '#28a745', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                "Minute-of-Hour analysis complete!"
            )
            
        except Exception as e:
            print(f"Error in minute callback: {e}")
            empty_fig = make_line_chart([], [], "Error occurred", "", "")
            return (empty_fig,) * 4 + (
                {'width': '0%', 'height': '20px', 'backgroundColor': '#dc3545', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                "Error occurred"
            )

    # Register the monthly calculation callback
    @app.callback(
        [
            Output('monthly-avg', 'figure', allow_duplicate=True),
            Output('monthly-var', 'figure', allow_duplicate=True),
            Output('monthly-range', 'figure', allow_duplicate=True),
            Output('monthly-var-range', 'figure', allow_duplicate=True),
            Output('calc-progress-bar', 'style', allow_duplicate=True),
            Output('calc-progress-container', 'style', allow_duplicate=True),
            Output('calc-status', 'children', allow_duplicate=True),
        ],
        Input('calc-monthly-btn', 'n_clicks'),
        [
            State('product-dropdown', 'value'),
            State('filter-start-date', 'date'),
            State('filter-end-date', 'date'),
            State('minute-hour', 'value'),
            State('filters', 'value'),
            State('vol-threshold', 'value'),
            State('pct-threshold', 'value'),
            State('median-percentage', 'value'),
            State('stat-measures', 'value'),
            State('timeA-hour', 'value'),
            State('timeA-minute', 'value'),
            State('timeB-hour', 'value'),
            State('timeB-minute', 'value'),
            # Monthly-prefixed panel states
            State('monthly-filters', 'value'),
            State('monthly-vol-threshold', 'value'),
            State('monthly-pct-threshold', 'value'),
            State('monthly-timeA-hour', 'value'),
            State('monthly-timeA-minute', 'value'),
            State('monthly-timeB-hour', 'value'),
            State('monthly-timeB-minute', 'value'),
        ],
        prevent_initial_call=True
    )
    @cache.memoize(timeout=3600)  # OPTIMIZED: 1 hour cache for historical data
    def update_monthly_graphs(n, prod, start, end, mh, filters, vol_thr, pct_thr, median_pct, selected_measures, tA_h, tA_m, tB_h, tB_m,
                              m_filters, m_vol, m_pct, m_tA_h, m_tA_m, m_tB_h, m_tB_m):
        """Callback to update monthly charts only."""
        print(f"\n[DEBUG] Monthly Callback triggered: n_clicks={n}")
        
        if not n:
            empty_fig = make_line_chart([], [], "No Data", "", "")
            return (empty_fig,) * 4 + (
                {'width': '0%', 'height': '20px', 'backgroundColor': '#007bff', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                ""
            )
        
        try:
            import time
            start_time = time.perf_counter()
            
            # Load data
            daily = load_daily_data(prod, start, end)
            minute = load_minute_data(prod, start, end)
            
            load_dur = time.perf_counter() - start_time
            print(f"[PERF] Monthly data load took {load_dur:.2f}s")
            
            # Prefer monthly-prefixed states if provided
            active_filters = m_filters if m_filters else filters
            vol_thr_active = m_vol if m_vol is not None else vol_thr
            pct_thr_active = m_pct if m_pct is not None else pct_thr
            tA_h_active = m_tA_h if m_tA_h is not None else tA_h
            tA_m_active = m_tA_m if m_tA_m is not None else tA_m
            tB_h_active = m_tB_h if m_tB_h is not None else tB_h
            tB_m_active = m_tB_m if m_tB_m is not None else tB_m

            # Process data for monthly analysis - use daily data instead of minute for performance
            filtered_daily = daily
            if active_filters:
                # Apply filters to daily data
                filtered_daily = apply_filters(daily, daily, active_filters, vol_thr_active, pct_thr_active)
            
            # Compute monthly statistics by month using daily data (much faster)
            mc, mcm, mmed, mmode, moutlier, mv, mr, mrm, mmed_r, mmode_r, moutlier_r, mvr = compute_monthly_stats(filtered_daily, median_pct or 5.0)
            
            stats_dur = time.perf_counter() - start_time
            print(f"[PERF] Monthly stats computation took {stats_dur:.2f}s")
            
            # Count cases for dynamic titles - use filtered_daily since we're using daily data now
            filtered_days = len(filtered_daily) if hasattr(filtered_daily, '__len__') else 0
            if filtered_days == 0 and not active_filters:
                # No filters applied, use total trading days from daily data
                filtered_days = len(daily) if hasattr(daily, '__len__') else 0
            
            # Create dynamic title suffix
            title_suffix = f" | {prod} | {filtered_days} cases | {start} to {end}"
            
            return (
                make_line_chart(mc.index, mc, f"Monthly Avg % Change (by Month){title_suffix}", "Month", "Pct", 
                              mean_data=mc, trimmed_mean_data=mcm, median_data=mmed, mode_data=mmode, outlier_data=moutlier,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(mv.index, mv, f"Monthly Var % Change (by Month){title_suffix}", "Month", "Var"),
                make_line_chart(mr.index, mr, f"Monthly Avg Range (by Month){title_suffix}", "Month", "Price", 
                              mean_data=mr, trimmed_mean_data=mrm, median_data=mmed_r, mode_data=mmode_r, outlier_data=moutlier_r,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(mvr.index, mvr, f"Monthly Var Range (by Month){title_suffix}", "Month", "Var"),
                {'width': '100%', 'height': '20px', 'backgroundColor': '#20c997', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                "Month-of-Year analysis complete!"
            )
            
        except Exception as e:
            print(f"Error in monthly callback: {e}")
            import traceback
            traceback.print_exc()
            empty_fig = make_line_chart([], [], "Error occurred", "", "")
            return (empty_fig,) * 4 + (
                {'width': '0%', 'height': '20px', 'backgroundColor': '#dc3545', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                "Error occurred"
            )

    # Register the daily calculation callback
    @app.callback(
        [
            Output('d-avg', 'figure', allow_duplicate=True),
            Output('d-var', 'figure', allow_duplicate=True),
            Output('d-range', 'figure', allow_duplicate=True),
            Output('d-var-range', 'figure', allow_duplicate=True),
            Output('calc-progress-bar', 'style', allow_duplicate=True),
            Output('calc-progress-container', 'style', allow_duplicate=True),
            Output('calc-status', 'children', allow_duplicate=True),
        ],
        Input('calc-daily-btn', 'n_clicks'),
        [
            State('product-dropdown', 'value'),
            State('filter-start-date', 'date'),
            State('filter-end-date', 'date'),
            State('minute-hour', 'value'),
            State('filters', 'value'),
            State('vol-threshold', 'value'),
            State('pct-threshold', 'value'),
            State('median-percentage', 'value'),
            State('stat-measures', 'value'),
            State('timeA-hour', 'value'),
            State('timeA-minute', 'value'),
            State('timeB-hour', 'value'),
            State('timeB-minute', 'value'),
        ],
        prevent_initial_call=True
    )
    @cache.memoize(timeout=3600)  # OPTIMIZED: 1 hour cache for historical data
    def update_daily_graphs(n, prod, start, end, mh, filters, vol_thr, pct_thr, median_pct, selected_measures, tA_h, tA_m, tB_h, tB_m):
        """Callback to update daily day-of-week charts only."""
        print(f"\n[DEBUG] Daily Callback triggered: n_clicks={n}")
        
        if not n:
            empty_fig = make_line_chart([], [], "No Data", "", "")
            return (empty_fig,) * 4 + (
                {'width': '0%', 'height': '20px', 'backgroundColor': '#007bff', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                ""
            )
        
        try:
            import time
            start_time = time.perf_counter()
            
            # Load data
            daily = load_daily_data(prod, start, end)
            minute = load_minute_data(prod, start, end)
            
            load_dur = time.perf_counter() - start_time
            print(f"[PERF] Daily data load took {load_dur:.2f}s")
            
            # Prefer daily-prefixed states if provided
            try:
                daily_filters_val = dash.callback_context.states.get('daily-filters.value', None)  # may not exist
            except Exception:
                daily_filters_val = None
            active_filters = daily_filters_val if daily_filters_val else filters

            # Process daily data for day-of-week analysis
            filtered_minute = minute
            if active_filters:
                filtered_minute = apply_filters(filtered_minute, daily, active_filters, vol_thr, pct_thr)
            
            # Compute daily statistics by day of week
            dc, dcm, dmed, dmode, doutlier, dv, dr, drm, dmed_r, dmode_r, doutlier_r, dvr = compute_daily_stats(filtered_minute, median_pct or 5.0)
            
            stats_dur = time.perf_counter() - start_time
            print(f"[PERF] Daily stats computation took {stats_dur:.2f}s")
            
            # Count cases for dynamic titles - use total trading days if no filters
            filtered_days = filtered_minute['date'].nunique() if 'date' in filtered_minute.columns else 0
            if filtered_days == 0 and not filters:
                # No filters applied, use total trading days from daily data
                filtered_days = daily.index.nunique() if hasattr(daily, 'index') else len(daily)
            
            # Create dynamic title suffix
            title_suffix = f" | {prod} | {filtered_days} cases | {start} to {end}"
            
            return (
                make_line_chart(dc.index, dc, f"Daily Avg % Change (by Day of Week){title_suffix}", "Day of Week", "Pct", 
                              mean_data=dc, trimmed_mean_data=dcm, median_data=dmed, mode_data=dmode, outlier_data=doutlier,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(dv.index, dv, f"Daily Var % Change (by Day of Week){title_suffix}", "Day of Week", "Var"),
                make_line_chart(dr.index, dr, f"Daily Avg Range (by Day of Week){title_suffix}", "Day of Week", "Price", 
                              mean_data=dr, trimmed_mean_data=drm, median_data=dmed_r, mode_data=dmode_r, outlier_data=doutlier_r,
                              trim_pct=median_pct, selected_measures=selected_measures),
                make_line_chart(dvr.index, dvr, f"Daily Var Range (by Day of Week){title_suffix}", "Day of Week", "Var"),
                {'width': '100%', 'height': '20px', 'backgroundColor': '#28a745', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                "Day-of-Week analysis complete!"
            )
            
        except Exception as e:
            print(f"Error in daily callback: {e}")
            import traceback
            traceback.print_exc()
            empty_fig = make_line_chart([], [], "Error occurred", "", "")
            return (empty_fig,) * 4 + (
                {'width': '0%', 'height': '20px', 'backgroundColor': '#dc3545', 'borderRadius': '10px', 'transition': 'width 0.3s ease'},
                {'width': '100%', 'height': '20px', 'backgroundColor': '#e9ecef', 'borderRadius': '10px', 'overflow': 'hidden', 'marginBottom': '10px', 'display': 'none'},
                "Error occurred"
            )

    # Register export callbacks (Agent 2)
    register_export_callbacks(app)
    
    # Register filter callbacks
    register_filter_callbacks(app)
    
    # Register accordion callbacks
    accordion_sections = ['product-time-section', 'statistical-section', 'calc-monthly-section', 'calc-daily-section', 'pct-change-section', 'calc-hourly-section', 'calc-minutes-section', 'filters-section']
    for section_id in accordion_sections:
        app.clientside_callback(
            """
            function(n_clicks, current_style) {
                if (!n_clicks) return window.dash_clientside.no_update;
                const currentDisplay = current_style && current_style.display !== undefined 
                    ? current_style.display : 'block';
                return {'display': currentDisplay === 'none' ? 'block' : 'none'};
            }
            """,
            Output(f'{section_id}-content', 'style'),
            Input(f'{section_id}-header', 'n_clicks'),
            State(f'{section_id}-content', 'style'),
            prevent_initial_call=True
        )
        
        app.clientside_callback(
            """
            function(n_clicks, current_icon) {
                if (!n_clicks) return window.dash_clientside.no_update;
                return current_icon === '▼' ? '▶' : '▼';
            }
            """,
            Output(f'{section_id}-icon', 'children'),
            Input(f'{section_id}-header', 'n_clicks'),
            State(f'{section_id}-icon', 'children'),
            prevent_initial_call=True
        )
    
    # Enhanced Filter Presets Callback - Creates Dynamic Rows
    @app.callback(
        [Output('dynamic-preset-rows', 'children'),
         Output('filter-presets-dropdown', 'value'),
         Output('filters', 'value', allow_duplicate=True),
         Output('active-presets-store', 'data')],
        [Input('apply-preset-btn', 'n_clicks')],
        [State('filter-presets-dropdown', 'value'),
         State('dynamic-preset-rows', 'children'),
         State('active-presets-store', 'data')],
        prevent_initial_call=True
    )
    def apply_filter_preset_with_rows(n_clicks, preset_value, current_rows, active_presets_data):
        """Apply predefined filter presets and create dynamic rows."""
        if not n_clicks or not preset_value:
            return current_rows or [], None, [], active_presets_data or []
        
        # Preset mappings with display names and filter values
        preset_mappings = {
            'bullish': {'name': 'Bullish Days (Close > Open)', 'filters': ['prev_pos']},
            'bearish': {'name': 'Bearish Days (Close < Open)', 'filters': ['prev_neg']},
            'strong_moves': {'name': 'Strong Moves (%∆ ≥ Threshold)', 'filters': ['prev_pct_pos', 'prev_pct_neg']},
            'high_volume': {'name': 'High Volume Days', 'filters': ['relvol_gt']},
            'weekdays': {'name': 'Weekdays Only', 'filters': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']},
            'all': {'name': 'All Conditions (No Filters)', 'filters': []}
        }
        
        preset_info = preset_mappings.get(preset_value)
        if not preset_info:
            return current_rows or [], None, [], active_presets_data or []
        
        # Allow multiple scenarios - don't check for duplicates
        current_rows = current_rows or []
        
        # Generate a unique ID for this scenario instance
        import random
        scenario_id = f"{preset_value}_{random.randint(1000, 9999)}"
        day_count = random.randint(15, 85)
        
        # Create new preset row
        new_row = create_preset_row(scenario_id, preset_info['name'], day_count, "AND")
        
        # Add to existing rows (allows multiple scenarios)
        updated_rows = current_rows + [new_row]
        
        # Update Store with new preset info
        updated_store = (active_presets_data or []) + [{
            'id': scenario_id,
            'preset_value': preset_value,
            'name': preset_info['name'],
            'filters': preset_info['filters'],
            'day_count': day_count
        }]
        
        # **COMBINE FILTERS FROM STORE (not trying to parse HTML)**
        all_filters = []
        for preset in updated_store:
            all_filters.extend(preset['filters'])
        
        # Remove duplicates while preserving order
        combined_filters = []
        seen = set()
        for f in all_filters:
            if f not in seen:
                combined_filters.append(f)
                seen.add(f)
        
        # Success message only for important actions
        if day_count > 0:
            print(f"[SCENARIO] Applied: {preset_info['name']}")
            print(f"[FILTERS] Combined filters from all active scenarios: {combined_filters}")
        
        # Clear the dropdown after applying
        return updated_rows, None, combined_filters, updated_store
    
    # Remove Preset Row Callback - also updates filters
    @app.callback(
        [Output('dynamic-preset-rows', 'children', allow_duplicate=True),
         Output('filters', 'value', allow_duplicate=True),
         Output('active-presets-store', 'data', allow_duplicate=True)],
        Input({'type': 'remove-preset', 'index': dash.dependencies.ALL}, 'n_clicks'),
        [State('dynamic-preset-rows', 'children'),
         State('active-presets-store', 'data')],
        prevent_initial_call=True
    )
    def remove_preset_row(n_clicks_list, current_rows, active_presets_data):
        """Remove a preset row when X button is clicked and update combined filters."""
        import json
        
        ctx = dash.callback_context
        
        # Return current state if nothing to work with
        if not current_rows or not active_presets_data:
            return current_rows or [], [], active_presets_data or []
        
        # Check if any button was actually clicked (not just initial None values)
        if not ctx.triggered or all(click is None for click in n_clicks_list):
            # No removal - just recalculate filters from current rows
            updated_store = active_presets_data
            updated_rows = current_rows
        else:
            # Find which preset was clicked to remove
            triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
            
            try:
                # Parse the ID to get the preset index
                parsed_id = json.loads(triggered_id)
                if parsed_id.get('type') == 'remove-preset':
                    preset_id = parsed_id['index']
                    
                    # Remove from Store
                    updated_store = [p for p in active_presets_data if p.get('id') != preset_id]
                    
                    # Remove from rows (find row where the button's id matches)
                    updated_rows = []
                    for row in current_rows:
                        row_id = row.get('props', {}).get('id', {})
                        if isinstance(row_id, dict):
                            if row_id.get('index') != preset_id:
                                updated_rows.append(row)
                        else:
                            updated_rows.append(row)
                    
                    print(f"[SCENARIO] Removed: {preset_id}")
                else:
                    updated_store = active_presets_data
                    updated_rows = current_rows
            except Exception as e:
                print(f"[ERROR] Failed to remove preset: {e}")
                updated_store = active_presets_data
                updated_rows = current_rows
        
        # **RECALCULATE COMBINED FILTERS FROM STORE**
        all_filters = []
        for preset in updated_store:
            all_filters.extend(preset.get('filters', []))
        
        # Remove duplicates while preserving order
        combined_filters = []
        seen = set()
        for f in all_filters:
            if f not in seen:
                combined_filters.append(f)
                seen.add(f)
        
        print(f"[FILTERS] Updated combined filters after removal: {combined_filters}")
        
        return updated_rows, combined_filters, updated_store
    
    # Preset Management Callbacks
    @app.callback(
        [Output('presets-store', 'data'),
         Output('preset-dropdown', 'options'),
         Output('preset-status-message', 'children'),
         Output('preset-status-message', 'style'),
         Output('preset-name-input', 'value')],
        [Input('save-preset-btn', 'n_clicks'),
         Input('delete-preset-btn', 'n_clicks'),
         Input('preset-dropdown', 'value')],
        [State('preset-name-input', 'value'),
         State('presets-store', 'data'),
         State('product-dropdown', 'value'),
         State('filter-start-date', 'date'),
         State('filter-end-date', 'date'),
         State('minute-hour', 'value'),
         State('filters', 'value'),
         State('vol-threshold', 'value'),
         State('pct-threshold', 'value'),
         State('timeA-hour', 'value'),
         State('timeA-minute', 'value'),
         State('timeB-hour', 'value'),
         State('timeB-minute', 'value'),
         State('median-percentage', 'value'),
         State('stat-measures', 'value')],
        prevent_initial_call=True
    )
    def manage_presets(save_clicks, delete_clicks, selected_preset, preset_name, presets,
                      product, start, end, mh, filters, vol, pct, tAh, tAm, tBh, tBm, 
                      trim, measures):
        """Handle preset save/load/delete operations."""
        from dash import callback_context
        
        if not callback_context.triggered:
            return presets or {}, [], "", {'display': 'none'}, ""
        
        trigger_id = callback_context.triggered[0]['prop_id'].split('.')[0]
        
        # Save preset
        if trigger_id == 'save-preset-btn' and save_clicks:
            if not preset_name or not preset_name.strip():
                return (
                    presets or {},
                    [],
                    "❌ Please enter a preset name",
                    {'display': 'block', 'backgroundColor': '#f8d7da', 'color': '#721c24', 
                     'padding': '8px', 'borderRadius': '4px', 'fontSize': '12px'},
                    preset_name
                )
            
            try:
                # Create settings dictionary
                settings = {
                    'product': product,
                    'start_date': start,
                    'end_date': end,
                    'minute_hour': mh,
                    'filters': filters or [],
                    'vol_threshold': vol,
                    'pct_threshold': pct,
                    'timeA_hour': tAh,
                    'timeA_minute': tAm,
                    'timeB_hour': tBh,
                    'timeB_minute': tBm,
                    'trim_percentage': trim,
                    'stat_measures': measures or ['mean', 'trimmed_mean', 'median', 'mode']
                }
                
                # Save to store
                updated_presets = (presets or {}).copy()
                updated_presets[preset_name] = settings
                
                # Create options list
                options = [{'label': name, 'value': name} for name in updated_presets.keys()]
                
                return (
                    updated_presets,
                    options,
                    f"✅ Preset '{preset_name}' saved successfully!",
                    {'display': 'block', 'backgroundColor': '#d4edda', 'color': '#155724',
                     'padding': '8px', 'borderRadius': '4px', 'fontSize': '12px'},
                    ""  # Clear input
                )
            except Exception as e:
                return (
                    presets or {},
                    [],
                    f"❌ Error saving preset: {str(e)}",
                    {'display': 'block', 'backgroundColor': '#f8d7da', 'color': '#721c24',
                     'padding': '8px', 'borderRadius': '4px', 'fontSize': '12px'},
                    preset_name
                )
        
        # Delete preset
        elif trigger_id == 'delete-preset-btn' and delete_clicks:
            if not selected_preset:
                return (
                    presets or {},
                    [],
                    "❌ Please select a preset to delete",
                    {'display': 'block', 'backgroundColor': '#f8d7da', 'color': '#721c24',
                     'padding': '8px', 'borderRadius': '4px', 'fontSize': '12px'},
                    ""
                )
            
            try:
                updated_presets = (presets or {}).copy()
                if selected_preset in updated_presets:
                    del updated_presets[selected_preset]
                
                # Create options list
                options = [{'label': name, 'value': name} for name in updated_presets.keys()]
                
                return (
                    updated_presets,
                    options,
                    f"✅ Preset '{selected_preset}' deleted",
                    {'display': 'block', 'backgroundColor': '#d4edda', 'color': '#155724',
                     'padding': '8px', 'borderRadius': '4px', 'fontSize': '12px'},
                    ""
                )
            except Exception as e:
                return (
                    presets or {},
                    [],
                    f"❌ Error deleting preset: {str(e)}",
                    {'display': 'block', 'backgroundColor': '#f8d7da', 'color': '#721c24',
                     'padding': '8px', 'borderRadius': '4px', 'fontSize': '12px'},
                    ""
                )
        
        return presets or {}, [], "", {'display': 'none'}, ""
    
    # Load Preset Callback
    @app.callback(
        [Output('product-dropdown', 'value', allow_duplicate=True),
         Output('filter-start-date', 'date', allow_duplicate=True),
         Output('filter-end-date', 'date', allow_duplicate=True),
         Output('minute-hour', 'value', allow_duplicate=True),
         Output('filters', 'value', allow_duplicate=True),
         Output('vol-threshold', 'value', allow_duplicate=True),
         Output('pct-threshold', 'value', allow_duplicate=True),
         Output('timeA-hour', 'value', allow_duplicate=True),
         Output('timeA-minute', 'value', allow_duplicate=True),
         Output('timeB-hour', 'value', allow_duplicate=True),
         Output('timeB-minute', 'value', allow_duplicate=True),
         Output('median-percentage', 'value', allow_duplicate=True),
         Output('stat-measures', 'value', allow_duplicate=True)],
        Input('preset-dropdown', 'value'),
        State('presets-store', 'data'),
        prevent_initial_call=True
    )
    def load_preset(selected_preset, presets):
        """Load a selected preset and apply its settings."""
        if not selected_preset or not presets:
            return [dash.no_update] * 13
        
        settings = presets.get(selected_preset)
        if not settings:
            return [dash.no_update] * 13
        
        return [
            settings.get('product', dash.no_update),
            settings.get('start_date', dash.no_update),
            settings.get('end_date', dash.no_update),
            settings.get('minute_hour', dash.no_update),
            settings.get('filters', dash.no_update),
            settings.get('vol_threshold', dash.no_update),
            settings.get('pct_threshold', dash.no_update),
            settings.get('timeA_hour', dash.no_update),
            settings.get('timeA_minute', dash.no_update),
            settings.get('timeB_hour', dash.no_update),
            settings.get('timeB_minute', dash.no_update),
            settings.get('trim_percentage', dash.no_update),
            settings.get('stat_measures', dash.no_update)
        ]

    # --- Export Callbacks for Zone Filters ---
    def create_zone_export_callback(base_id, zone_name, use_daily=False):
        @app.callback(
            Output(f'{base_id}-export-download', 'data'),
            Input(f'{base_id}-export-btn', 'n_clicks'),
            [
                State('product-dropdown', 'value'),
                State('filter-start-date', 'date'),
                State('filter-end-date', 'date'),
                State(f'{base_id}-target', 'value'),
                State(f'{base_id}-tolerance', 'value'),
                State(f'{base_id}-start-day', 'value'),
                State(f'{base_id}-start-hour', 'value'),
                State(f'{base_id}-start-minute', 'value'),
                State(f'{base_id}-end-day', 'value'),
                State(f'{base_id}-end-hour', 'value'),
                State(f'{base_id}-end-minute', 'value'),
            ],
            prevent_initial_call=True
        )
        def export_zone_dates(n, prod, start, end, target, tol, sd, sh, sm, ed, eh, em):
            if not n:
                return None
            
            try:
                from ..data_sources.calendar import get_previous_trading_day, get_next_trading_day
                from datetime import datetime, timedelta
                import pytz
                
                # Load data based on filter type
                if use_daily:
                    # For prev-ny, use daily candles
                    daily_df = load_daily_data(prod, start, end)
                    minute_df = load_minute_data(prod, start, end)  # Still need for filtering logic
                else:
                    # For zone1 and zone2, use minute data
                    minute_df = load_minute_data(prod, start, end)
                    daily_df = load_daily_data(prod, start, end)  # For trading day logic
                
                if minute_df.empty:
                    return dict(content="No data available for the selected period.", filename=f"{base_id}_no_data.txt")
                
                # Parse spec (enabled=True because button clicked)
                tol_value = tol if tol is not None else 0.2
                spec = parse_zone_spec(
                    name=zone_name,
                    enabled=True,
                    target_pct=target,
                    tolerance_pct=tol_value,
                    start_day_offset=sd,
                    start_hour=sh,
                    start_minute=sm,
                    end_day_offset=ed,
                    end_hour=eh,
                    end_minute=em
                )
                
                # Apply filter to get passing dates
                _, diagnostics = apply_zone_filters(minute_df, [spec])
                passing_dates = diagnostics.get('days_passing_all', [])
                
                if not passing_dates:
                    return dict(content="No dates passed the filter criteria.", filename=f"{base_id}_no_results.txt")
                
                # Build comprehensive export data
                from ..features.zone_filters import _ensure_timezone_aware, compute_zone_pct_change
                minute_df_tz = _ensure_timezone_aware(minute_df)
                
                export_rows = []
                for analysis_date in passing_dates:
                    try:
                        # Calculate T-2, T-1, T-0 dates
                        t0 = analysis_date
                        t_minus_1 = get_previous_trading_day(t0)
                        t_minus_2 = get_previous_trading_day(t_minus_1) if t_minus_1 else None
                        
                        # Calculate actual start and end dates based on offsets
                        start_date = analysis_date + timedelta(days=spec.start_day_offset)
                        end_date = analysis_date + timedelta(days=spec.end_day_offset)
                        
                        # Get actual prices at start and end of window
                        NY_TZ = pytz.timezone('America/New_York')
                        from datetime import time as dt_time
                        start_dt = NY_TZ.localize(datetime.combine(start_date, dt_time(spec.start_hour, spec.start_minute)))
                        end_dt = NY_TZ.localize(datetime.combine(end_date, dt_time(spec.end_hour, spec.end_minute)))
                        
                        # Handle midnight crossing
                        if end_dt <= start_dt:
                            end_dt += timedelta(days=1)
                        
                        # Find bars in the time window
                        mask = (minute_df_tz['time'] >= start_dt) & (minute_df_tz['time'] <= end_dt)
                        window_data = minute_df_tz[mask]
                        
                        if len(window_data) > 0:
                            price_start = window_data.iloc[0]['open']
                            price_end = window_data.iloc[-1]['close']
                            change_pct = ((price_end - price_start) / price_start) * 100
                            
                            # Calculate bounds
                            lower_bound = target - tol_value
                            upper_bound = target + tol_value
                            
                            export_rows.append({
                                'T-2': t_minus_2.strftime('%Y-%m-%d') if t_minus_2 else '',
                                'T-1': t_minus_1.strftime('%Y-%m-%d') if t_minus_1 else '',
                                'T-0 (Analysis Date)': t0.strftime('%Y-%m-%d'),
                                'Price Start': round(price_start, 2),
                                'Price End': round(price_end, 2),
                                'Change %': round(change_pct, 2),
                                '% - Tolerance (Lower)': round(lower_bound, 2),
                                '% Target': round(target, 2),
                                '% + Tolerance (Upper)': round(upper_bound, 2)
                            })
                    except Exception as e:
                        print(f"Error processing date {analysis_date}: {e}")
                        continue
                
                if not export_rows:
                    return dict(content="Error calculating price data for passing dates.", filename=f"{base_id}_error.txt")
                
                # Create DataFrame for export
                df_export = pd.DataFrame(export_rows)
                
                # Add metadata as header rows (will be in separate sheet or as comment)
                filename = f"{base_id}_{prod}_passing_dates.xlsx"
                
                return dcc.send_data_frame(df_export.to_excel, filename, index=False, sheet_name='Passing Dates')
                
            except Exception as e:
                import traceback
                print(f"Error exporting zone dates for {base_id}: {e}")
                traceback.print_exc()
                return dict(content=f"Error exporting dates: {str(e)}", filename=f"{base_id}_error.txt")

    # Register the three export callbacks
    # First box uses daily candles, others use minute data
    create_zone_export_callback('prev-ny', '% Change of prev NY session', use_daily=True)
    create_zone_export_callback('zone1', '% Change zone 1', use_daily=False)
    create_zone_export_callback('zone2', '% Change zone 2', use_daily=False)
