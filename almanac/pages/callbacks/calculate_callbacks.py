"""
Callback Functions for Profile Page

Contains all callback functions for the profile page, organized by functionality.
"""

from dash import dcc, html, Input, Output, State, callback_context
import dash
import dash.dependencies
import pandas as pd
import math
from datetime import datetime
import pytz
import logging
import time

from ...data_sources import load_minute_data, load_daily_data
from ...features import (
    compute_hourly_stats,
    compute_minute_stats,
    apply_filters,
    apply_time_filters,
)
from ...features.hod_lod import (
    detect_hod_lod,
    compute_survival_curves,
    compute_hod_lod_heatmap,
    compute_rolling_median_time,
    compute_trend_test,
)
from ...viz import make_line_chart

logger = logging.getLogger(__name__)


def _get_container_visibility(button_id=None):
    """
    Determine which graph containers should be visible based on the button clicked.
    
    Args:
        button_id (str): ID of the button that was clicked
        
    Returns:
        tuple: Style dictionaries for container visibility
    """
    if button_id == 'calc-btn':
        return {'display': 'block'}, {'display': 'block'}
    else:
        return {'display': 'none'}, {'display': 'none'}


def _scale_variance(v):
    """
    Scale variance values for better visualization.
    
    Args:
        v (float): Variance value to scale
        
    Returns:
        float: Scaled variance value
    """
    if v is None or pd.isna(v):
        return 0
    return min(v * 1000, 100)  # Scale and cap at 100


def _generate_summary(daily, filtered_minute, hc, hv, hr, hvr, mc, mv, mr, mvr, sh, sm):
    """
    Generate a summary of the analysis results.
    
    Args:
        daily (pd.DataFrame): Daily data
        filtered_minute (pd.DataFrame): Filtered minute data
        hc, hv, hr, hvr: Hourly statistics
        mc, mv, mr, mvr: Minute statistics
        sh, sm: Chart figures
        
    Returns:
        html.Div: Summary component
    """
    try:
        total_days = len(daily)
        filtered_cases = len(filtered_minute)
        
        summary_content = [
            html.H4("Analysis Summary", style={'marginBottom': '15px'}),
            html.P(f"Total Days Analyzed: {total_days}"),
            html.P(f"Filtered Cases: {filtered_cases}"),
            html.P(f"Hourly Data Points: {len(hc)}"),
            html.P(f"Minute Data Points: {len(mc)}")
        ]
        
        return html.Div(summary_content)
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        return html.Div("Error generating summary")


def register_calculation_callbacks(app, cache):
    """
    Register calculation-related callbacks.
    
    Args:
        app: Dash app instance
        cache: Cache instance for memoization
    """
    
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
            Output('hourly-graphs-container', 'style'),
            Output('hod-lod-container', 'style'),
        ],
        Input('calc-btn', 'n_clicks'),
        [
            State('product-dropdown', 'value'),
            State('filter-start-date', 'date'),
            State('filter-end-date', 'date'),
            State('filters', 'value'),
            State('vol-threshold', 'value'),
            State('pct-threshold', 'value'),
        ]
    )
    @cache.memoize(timeout=3600) if cache else lambda f: f  # OPTIMIZED: 1 hour cache
    def update_graphs(n_clicks, prod, start, end, filters, vol_thr, pct_thr):
        """
        Main callback to update all charts and summary.
        
        Args:
            n_clicks (int): Number of button clicks
            prod (str): Product symbol
            start (str): Start date
            end (str): End date
            filters (list): List of active filters
            vol_thr (float): Volume threshold
            pct_thr (float): Percentage threshold
            
        Returns:
            tuple: Chart figures and summary components
        """
        logger.info(f"Callback triggered: n_clicks={n_clicks}")
        logger.info(f"Parameters: prod={prod}, start={start}, end={end}")
        
        # Initialize empty figures
        empty_fig = make_line_chart([], [], "No Data", "", "")
        empty_kpi = html.Div("Initializing...")
        
        # Don't run if button hasn't been clicked
        if not n_clicks:
            logger.info("No button clicks, returning empty state")
            return (empty_fig,) * 8 + (html.Div("Click Calculate to see results"), empty_kpi) + (empty_fig,) * 6 + ("0 cases",) + _get_container_visibility(None)
        
        # Simple progress indicator
        progress_msg = html.Div([
            html.Span("⏳ Processing...", style={'color': '#007bff', 'fontWeight': 'bold'}),
            html.Br(),
            html.Small("This may take a few moments for large datasets", style={'color': '#666'})
        ])
        
        try:
            # Validate inputs
            if not prod or not start or not end:
                logger.warning("Missing required parameters")
                return (empty_fig,) * 8 + (html.Div("Missing required parameters"), empty_kpi) + (empty_fig,) * 6 + ("0 cases",) + _get_container_visibility('calc-btn')
            
            # Load data with enhanced error handling
            try:
                logger.info("Loading data...")
                daily = load_daily_data(prod, start, end)
                minute = load_minute_data(prod, start, end)
                logger.info(f"Loaded daily={len(daily)} rows, minute={len(minute)} rows")
            except Exception as db_error:
                logger.warning(f"Database unavailable, using demo data: {db_error}")
                try:
                    from ...data_sources import generate_demo_daily_data, generate_demo_minute_data
                    daily = generate_demo_daily_data(prod, start, end)
                    minute = generate_demo_minute_data(prod, start, end)
                    logger.info("Successfully loaded demo data")
                except Exception as demo_error:
                    logger.error(f"Failed to load demo data: {demo_error}")
                    error_msg = html.Div([
                        html.B("Data Loading Error:", style={'color': 'red'}),
                        html.Br(),
                        f"Unable to load data for {prod}. Please try again later."
                    ])
                    return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("0 cases",) + _get_container_visibility('calc-btn')
            
            # Check if we have data
            if daily.empty or minute.empty:
                error_msg = html.Div([
                    html.B("Error:", style={'color': 'red'}),
                    html.Br(),
                    f"No data found for {prod} between {start} and {end}"
                ])
                return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("0 cases",) + _get_container_visibility('calc-btn')
            
            # Apply filtering
            filtered_minute = minute
            if filters:
                try:
                    filtered_minute = apply_filters(filtered_minute, daily, filters, vol_thr, pct_thr)
                    logger.info(f"Applied filters: {len(minute)} -> {len(filtered_minute)} rows")
                except Exception as filter_error:
                    logger.error(f"Filter error: {filter_error}")
                    raise
            
            # Compute statistics with enhanced error handling
            try:
                logger.info("Computing statistics...")
                hc, hv, hr, hvr = compute_hourly_stats(filtered_minute, daily)
                mc, mv, mr, mvr = compute_minute_stats(filtered_minute, daily)
                logger.info(f"Computed stats: hourly={len(hc)} hours, minute={len(mc)} minutes")
            except Exception as stats_error:
                logger.error(f"Stats computation error: {stats_error}")
                error_msg = html.Div([
                    html.B("Statistics Error:", style={'color': 'red'}),
                    html.Br(),
                    f"Unable to compute statistics. Error: {str(stats_error)[:100]}..."
                ])
                return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("0 cases",) + _get_container_visibility('calc-btn')
            
            # Generate charts with enhanced error handling
            try:
                logger.info("Generating charts...")
                sh = make_line_chart(hc.index, hc.values, f"{prod} Hourly Average", "Hour", "Price")
                sv = make_line_chart(hv.index, hv.values, f"{prod} Hourly Variance", "Hour", "Variance")
                sr = make_line_chart(hr.index, hr.values, f"{prod} Hourly Range", "Hour", "Range")
                svr = make_line_chart(hvr.index, hvr.values, f"{prod} Hourly Variance Range", "Hour", "Variance Range")
                
                sm = make_line_chart(mc.index, mc.values, f"{prod} Minute Average", "Minute", "Price")
                smv = make_line_chart(mv.index, mv.values, f"{prod} Minute Variance", "Minute", "Variance")
                smr = make_line_chart(mr.index, mr.values, f"{prod} Minute Range", "Minute", "Range")
                smvr = make_line_chart(mvr.index, mvr.values, f"{prod} Minute Variance Range", "Minute", "Variance Range")
                
                logger.info("Generated charts successfully")
            except Exception as chart_error:
                logger.error(f"Chart generation error: {chart_error}")
                error_msg = html.Div([
                    html.B("Chart Generation Error:", style={'color': 'red'}),
                    html.Br(),
                    f"Unable to generate charts. Error: {str(chart_error)[:100]}..."
                ])
                return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("0 cases",) + _get_container_visibility('calc-btn')
            
            # HOD/LOD Analysis with enhanced error handling
            try:
                logger.info("Starting HOD/LOD analysis...")
                hod_lod_df = detect_hod_lod(daily)
                logger.info(f"Detected HOD/LOD for {len(hod_lod_df)} days")
                
                if len(hod_lod_df) < 10:
                    logger.warning(f"Insufficient HOD/LOD data ({len(hod_lod_df)} days)")
                    hod_survival_fig = lod_survival_fig = empty_fig
                    hod_heatmap_fig = lod_heatmap_fig = empty_fig
                    hod_rolling_fig = lod_rolling_fig = empty_fig
                    hod_lod_kpi = html.Div("Insufficient HOD/LOD data")
                else:
                    hod_survival_fig, lod_survival_fig = compute_survival_curves(hod_lod_df)
                    hod_heatmap_fig, lod_heatmap_fig = compute_hod_lod_heatmap(hod_lod_df)
                    hod_rolling_fig, lod_rolling_fig = compute_rolling_median_time(hod_lod_df)
                    
                    trend_result = compute_trend_test(hod_lod_df)
                    hod_lod_kpi = html.Div([
                        html.H4("HOD/LOD Analysis", style={'marginBottom': '10px'}),
                        html.P(f"Total Cases: {len(hod_lod_df)}"),
                        html.P(f"Trend Test: {trend_result}")
                    ])
            except Exception as hod_lod_error:
                logger.error(f"HOD/LOD analysis error: {hod_lod_error}")
                hod_survival_fig = lod_survival_fig = empty_fig
                hod_heatmap_fig = lod_heatmap_fig = empty_fig
                hod_rolling_fig = lod_rolling_fig = empty_fig
                hod_lod_kpi = html.Div([
                    html.B("HOD/LOD Analysis Error:", style={'color': 'orange'}),
                    html.Br(),
                    f"Analysis failed: {str(hod_lod_error)[:50]}..."
                ])
            
            # Generate summary
            summary = _generate_summary(daily, filtered_minute, hc, hv, hr, hvr, mc, mv, mr, mvr, sh, sm)
            
            return (sh, sv, sr, svr, sm, smv, smr, smvr, 
                   summary, hod_lod_kpi,
                   hod_survival_fig, lod_survival_fig, hod_heatmap_fig, lod_heatmap_fig, hod_rolling_fig, lod_rolling_fig,
                   f"{len(filtered_minute)} cases",) + _get_container_visibility('calc-btn')
        
        except Exception as e:
            logger.error(f"Callback error: {e}")
            error_msg = html.Div([
                html.B("Error:", style={'color': 'red'}),
                html.Br(),
                str(e)
            ])
            return (empty_fig,) * 8 + (error_msg, empty_kpi) + (empty_fig,) * 6 + ("0 cases",) + _get_container_visibility('calc-btn')


def register_ui_callbacks(app):
    """
    Register UI-related callbacks.
    
    Args:
        app: Dash app instance
    """
    
    # Simple progress indicator callback
    @app.callback(
        Output('progress-indicator', 'style'),
        Input('calc-btn', 'n_clicks'),
        prevent_initial_call=True
    )
    def show_progress_indicator(n_clicks):
        """
        Show progress indicator when calculate button is clicked.
        
        Args:
            n_clicks (int): Number of button clicks
            
        Returns:
            dict: Style for progress indicator
        """
        if n_clicks and n_clicks > 0:
            return {'display': 'block', 'textAlign': 'center', 'marginTop': '10px'}
        else:
            return {'display': 'none'}
    
    @app.callback(
        Output('current-time-display', 'children'),
        Input('current-time-display', 'id'),
        prevent_initial_call=False
    )
    def update_current_time(_):
        """
        Update the current time display.
        
        Args:
            _: Unused input parameter
            
        Returns:
            str: Formatted current time
        """
        try:
            est = pytz.timezone('US/Eastern')
            current_time = datetime.now(est)
            return current_time.strftime('%H:%M:%S')
        except Exception as e:
            logger.error(f"Error updating time: {e}")
            return "Error"
    
    @app.callback(
        [Output('filter-start-date', 'date', allow_duplicate=True),
         Output('filter-end-date', 'date', allow_duplicate=True)],
        [Input('product-dropdown', 'value'),
         Input('max-date-range-btn', 'n_clicks')],
        prevent_initial_call='initial_duplicate'
    )
    def handle_date_range(product_value, max_range_clicks):
        """
        Set default date range when page loads or handle Max Date Range button.
        
        Args:
            product_value (str): Selected product
            max_range_clicks (int): Number of max range button clicks
            
        Returns:
            tuple: Start and end dates
        """
        try:
            ctx = dash.callback_context
            
            if not ctx.triggered:
                return '2010-01-01', '2025-04-08'
            
            trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
            
            if trigger_id == 'max-date-range-btn' and max_range_clicks:
                return '2010-01-01', '2025-12-31'
            elif trigger_id == 'product-dropdown':
                return '2010-01-01', '2025-04-08'
            
            return dash.no_update, dash.no_update
        except Exception as e:
            logger.error(f"Date range error: {e}")
            return '2010-01-01', '2025-04-08'


def register_progress_bar_callbacks(app):
    """
    Register progress bar callbacks for the calculate button.
    
    Args:
        app: Dash app instance
    """
    
    @app.callback(
        [
            Output('progress-container', 'style'),
            Output('progress-bar', 'style'),
            Output('progress-text', 'children'),
            Output('calc-btn', 'style')
        ],
        [
            Input('calc-btn', 'n_clicks'),
            Input('calc-btn', 'disabled')
        ],
        prevent_initial_call=True
    )
    def update_progress_bar(n_clicks, is_disabled):
        """
        Update progress bar visibility and state.
        
        Args:
            n_clicks: Number of button clicks
            is_disabled: Whether button is disabled
            
        Returns:
            tuple: Progress bar styles and text
        """
        ctx = callback_context
        
        if not ctx.triggered:
            return (
                {'display': 'none'},
                {'width': '0%', 'height': '100%', 'backgroundColor': '#007bff', 'borderRadius': '2px', 'transition': 'width 0.3s ease'},
                "Processing...",
                {'width': '100%', 'padding': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'border': 'none', 'borderRadius': '5px', 'cursor': 'pointer', 'fontSize': '16px', 'fontWeight': 'bold', 'transition': 'all 0.3s ease'}
            )
        
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if trigger_id == 'calc-btn' and n_clicks and n_clicks > 0:
            # Show progress bar and disable button
            return (
                {'display': 'block', 'marginTop': '8px'},
                {'width': '0%', 'height': '100%', 'backgroundColor': '#007bff', 'borderRadius': '2px', 'transition': 'width 0.3s ease', 'background': 'linear-gradient(90deg, #007bff 0%, #0056b3 100%)'},
                "Loading data...",
                {'width': '100%', 'padding': '10px', 'backgroundColor': '#6c757d', 'color': 'white', 'border': 'none', 'borderRadius': '5px', 'cursor': 'not-allowed', 'fontSize': '16px', 'fontWeight': 'bold', 'transition': 'all 0.3s ease', 'opacity': '0.7'}
            )
        else:
            # Hide progress bar and enable button
            return (
                {'display': 'none'},
                {'width': '0%', 'height': '100%', 'backgroundColor': '#007bff', 'borderRadius': '2px', 'transition': 'width 0.3s ease'},
                "Processing...",
                {'width': '100%', 'padding': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'border': 'none', 'borderRadius': '5px', 'cursor': 'pointer', 'fontSize': '16px', 'fontWeight': 'bold', 'transition': 'all 0.3s ease'}
            )
    
    @app.callback(
        [
            Output('progress-bar', 'style', allow_duplicate=True),
            Output('progress-text', 'children', allow_duplicate=True)
        ],
        [
            Input('calc-btn', 'n_clicks')
        ],
        prevent_initial_call=True
    )
    def animate_progress_bar(n_clicks):
        """
        Animate the progress bar during calculation.
        
        Args:
            n_clicks: Number of button clicks
            
        Returns:
            tuple: Progress bar style and text
        """
        if not n_clicks or n_clicks == 0:
            return dash.no_update, dash.no_update
        
        # Simulate progress stages
        progress_stages = [
            (10, "Loading data..."),
            (25, "Processing filters..."),
            (45, "Computing statistics..."),
            (70, "Generating charts..."),
            (90, "Finalizing results..."),
            (100, "Complete!")
        ]
        
        # For now, we'll show a simple progress animation
        # In a real implementation, you'd update this based on actual progress
        current_progress = 50  # This would be updated based on actual calculation progress
        current_text = "Processing..."
        
        for progress, text in progress_stages:
            if current_progress >= progress:
                current_text = text
        
        return (
            {
                'width': f'{current_progress}%',
                'height': '100%',
                'backgroundColor': '#007bff',
                'borderRadius': '2px',
                'transition': 'width 0.3s ease',
                'background': 'linear-gradient(90deg, #007bff 0%, #0056b3 100%)'
            },
            current_text
        )
