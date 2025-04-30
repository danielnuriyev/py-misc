# Import necessary libraries
import dash
from dash import dcc, html, Input, Output, State, clientside_callback, callback, no_update, ALL, ctx
from dash_iconify import DashIconify
import dash_bootstrap_components as dbc # Import Bootstrap components
import copy  # Needed to copy style dictionaries
import uuid  # To generate unique IDs for files
import json  # For printing data in submit callback (and potentially other uses)
import re # For sentence splitting
import zipfile # To handle zip files
import io # To handle zip files in memory
import base64 # To decode upload content
import os # To check file extensions
import time # For sleep in upload callback

import bedrock

# Initialize the Dash app
# suppress_callback_exceptions=True is needed for dynamically generated components and multi-output callbacks
app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

# --- Configuration Constants ---

# Textarea heights
initial_height_px = 48
max_height_px = 140

def models(current_chat_id = None):
    
    if current_chat_id:
        _chat = bedrock.Chat(current_chat_id)
    else:
        _chat = bedrock.Chat("local")

    model_options = [{"label": "auto", "value": "auto"}]
    model_options.extend([
        {"label": model, "value": model} for model in _chat.list_models()
    ])
    return model_options

# Side Panel Widths
panel_width_open = '25%'
panel_width_closed = '50px' # Minimal width to show icon
summary_max_length = 80 # Max characters for previous chat summary

# Allowed file extensions for direct upload and inside zip
ALLOWED_EXTENSIONS = {'.txt', '.yaml', '.sql', '.py', '.md'}
ALLOWED_EXTENSIONS_UPLOAD = ALLOWED_EXTENSIONS.union({'.zip'}) # Add .zip for upload component

# --- Helper Function ---
def get_summary(text, max_len=summary_max_length):
    """Extracts the first sentence or a truncated summary from text."""
    if not text:
        return "Chat (no text)"
    match = re.search(r'[^.!?]*[.!?]', text)
    if match:
        summary = match.group(0).strip()
    else:
        summary = text
    if len(summary) > max_len:
        summary = summary[:max_len].strip() + "..."
    elif not match and len(text) > len(summary):
         summary += "..."
    if not summary:
         return "Chat entry"
    return summary


# --- Define Styles ---
# (Styles remain unchanged)
transition_style = 'all 0.3s ease-in-out'
side_panel_base_style = {'height': '100vh', 'backgroundColor': '#f8f9fa', 'borderRight': '1px solid #dee2e6', 'overflowX': 'hidden', 'overflowY': 'auto', 'transition': transition_style, 'position': 'relative', 'flexShrink': 0, 'display': 'flex', 'flexDirection': 'column'}
side_panel_closed_style = copy.deepcopy(side_panel_base_style); side_panel_closed_style['width'] = panel_width_closed
side_panel_open_style = copy.deepcopy(side_panel_base_style); side_panel_open_style['width'] = panel_width_open
panel_toggle_button_style = {'position': 'absolute', 'top': '10px', 'right': '10px', 'border': 'none', 'background': 'none', 'padding': '5px', 'cursor': 'pointer', 'color': '#6c757d'}
side_panel_content_base_style = {'padding': '1em', 'transition': 'opacity 0.2s ease-in-out', 'opacity': 0, 'display': 'none', 'flexGrow': 1, 'overflowY': 'auto'}
side_panel_content_closed_style = copy.deepcopy(side_panel_content_base_style)
side_panel_content_open_style = copy.deepcopy(side_panel_content_base_style); side_panel_content_open_style['opacity'] = 1; side_panel_content_open_style['display'] = 'flex'; side_panel_content_open_style['flexDirection'] = 'column'
new_chat_button_style = {'width': '100%', 'padding': '10px', 'marginTop': '40px', 'fontSize': '1rem', 'textAlign': 'center', 'cursor': 'pointer', 'backgroundColor': '#007bff', 'color': 'white', 'border': 'none', 'borderRadius': '5px', 'flexShrink': 0}
previous_chats_container_style = {'marginTop': '10px', 'overflowY': 'auto', 'flexGrow': 1}
previous_chat_item_container_style = {'display': 'flex', 'alignItems': 'center', 'justifyContent': 'space-between', 'width': '100%', 'borderBottom': '1px solid #eee', 'padding': '2px 5px 2px 0px'}
previous_chat_button_style = {'flexGrow': 1, 'padding': '6px 10px', 'border': 'none', 'fontSize': '0.9em', 'cursor': 'pointer', 'textAlign': 'left', 'whiteSpace': 'nowrap', 'overflow': 'hidden', 'textOverflow': 'ellipsis', 'color': '#333', 'backgroundColor': 'transparent', 'borderRadius': '0px'}
previous_chat_action_button_style = {'border': 'none', 'background': 'none', 'color': '#aaa', 'cursor': 'pointer', 'padding': '5px', 'marginLeft': '2px', 'flexShrink': 0, 'lineHeight': '1'}
edit_summary_input_style = {'flexGrow': 1, 'padding': '6px 10px', 'fontSize': '0.9em', 'border': '1px solid #007bff', 'borderRadius': '3px', 'marginRight': '5px'}
chat_container_base_style = {
    'flexGrow': 1, 'height': '100vh', 'display': 'flex',
    'flexDirection': 'column', 'transition': transition_style, 'overflow': 'hidden',
    'justifyContent': 'flex-end'
}
chat_container_panel_closed_style = copy.deepcopy(chat_container_base_style)
chat_interface_style = {'display': 'flex', 'flexDirection': 'column', 'alignItems': 'flex-start', 'width': '70%', 'maxWidth': '900px', 'marginTop': 'auto', 'marginBottom': '20px', 'marginLeft': 'auto', 'marginRight': 'auto', 'height': '100%', 'maxHeight': '100vh'}
filename_row_style_base = {'display': 'flex', 'flexWrap': 'wrap', 'alignItems': 'center', 'marginTop': '0.2em', 'marginBottom': '0.2em', 'width': '100%'}
filename_row_style_visible = copy.deepcopy(filename_row_style_base)
filename_row_style_hidden = copy.deepcopy(filename_row_style_base); filename_row_style_hidden['display'] = 'none'
file_item_style = {'display': 'inline-flex', 'alignItems': 'center', 'border': '1px solid #ddd', 'borderRadius': '4px', 'padding': '0.1em 0.3em', 'margin': '0.2em', 'fontSize': '0.9em', 'color': '#333', 'backgroundColor': '#f8f8f8'}
filename_text_style = {'overflow': 'hidden', 'textOverflow': 'ellipsis', 'whiteSpace': 'nowrap', 'paddingRight': '1ch'}
action_button_style = {'border': 'none', 'background': 'none', 'padding': '0', 'margin': '0', 'verticalAlign': 'middle', 'lineHeight': '1', 'cursor': 'pointer', 'color': '#888', 'flexShrink': 0}
submit_button_style = copy.deepcopy(action_button_style); submit_button_style['color'] = '#007bff'
history_delete_button_style = copy.deepcopy(action_button_style); history_delete_button_style['color'] = '#aaa'
bottom_right_button_style = {'position': 'absolute', 'bottom': '5px', 'right': '5px', 'border': 'none', 'background': 'none', 'padding': '0', 'margin': '0', 'lineHeight': '1', 'cursor': 'pointer', 'color': '#aaa', 'opacity': '0.7', 'width': '16px', 'height': '16px', 'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'}
action_button_container_style = {'position': 'absolute', 'bottom': '5px', 'right': '5px', 'display': 'flex', 'alignItems': 'center', 'gap': '4px'}
action_icon_button_style = {'border': 'none', 'background': 'none', 'padding': '0', 'margin': '0', 'lineHeight': '1', 'cursor': 'pointer', 'color': '#aaa', 'opacity': '0.7', 'width': '16px', 'height': '16px', 'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'}
icon_row_style_base = {'display': 'flex', 'alignItems': 'center', 'width': '100%', 'marginTop': '1em'}
icon_row_style_no_margin = copy.deepcopy(icon_row_style_base); icon_row_style_no_margin['marginTop'] = '0.5em'
upload_component_style = {'border': 'none', 'padding': '0', 'display': 'inline-block', 'flexShrink': 0}
dbc_dropdown_style = {'margin-left': '10px', 'margin-right': '1em'} # Adjust spacing as needed
submitted_display_style = {'width': '100%', 'marginBottom': '10px', 'padding': '10px', 'overflowY': 'auto', 'fontSize': '0.95em', 'flexGrow': 1}
message_entry_style = {'marginBottom': '1.0em'}
message_qa_row_style = {'display': 'flex', 'alignItems': 'flex-start'}
q_box_style = {'width': '1.8em', 'height': '1.8em', 'backgroundColor': '#e0e0e0', 'color': '#333', 'borderRadius': '4px', 'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center', 'fontWeight': 'bold', 'marginRight': '0.6em', 'flexShrink': 0}
question_box_style = {'backgroundColor': '#f0f0f0', 'padding': '0.4em 0.7em 1.5em 0.7em', 'borderRadius': '4px', 'flexGrow': 1, 'whiteSpace': 'pre-wrap', 'wordBreak': 'break-word', 'position': 'relative'}
a_box_style = copy.deepcopy(q_box_style); a_box_style['backgroundColor'] = '#e0f0e0'
answer_box_style = {'backgroundColor': '#f8fff8', 'padding': '0.4em 0.7em 1.5em 0.7em', 'borderRadius': '4px', 'flexGrow': 1, 'whiteSpace': 'pre-wrap', 'wordBreak': 'break-word', 'position': 'relative'}
message_files_style = {'marginLeft': 'calc(1.8em + 0.6em)', 'marginTop': '0.3em', 'marginBottom': '0.3em', 'display': 'flex', 'flexWrap': 'wrap', 'gap': '5px'}
message_filename_style = {'fontSize': '0.85em', 'color': '#555', 'backgroundColor': '#eee', 'padding': '1px 5px', 'borderRadius': '3px', 'border': '1px solid #ddd'}
deletable_file_item_style = {'display': 'inline-flex', 'alignItems': 'center', 'border': '1px solid #ccc', 'borderRadius': '4px', 'padding': '1px 2px 1px 5px', 'margin': '0', 'fontSize': '0.85em', 'color': '#333', 'backgroundColor': '#f0f0f0'}
deletable_filename_text_style = {'paddingRight': '0.5ch'}
model_display_style = {
    'position': 'absolute', # Position relative to the answer box
    'bottom': '5px',        # Position it near the bottom
    'left': '10px',         # Position it from the left
    'fontSize': '0.8em',
    'color': '#888',
    'textAlign': 'left'
}

# --- Define App Layout ---
# (Layout remains unchanged)
app.layout = html.Div( # Outermost container
    [

        dcc.Store(id='side-panel-state-store', data={'isOpen': False}),
        dcc.Store(id='uploaded-files-store', data=[]),
        dcc.Store(id='message-history-store', data=[]),
        dcc.Store(id='previous-chats-store', data=[], storage_type='local'),
        dcc.Store(id='current-chat-id-store', data=None),
        dcc.Store(id='editing-summary-state-store', data={'editing_chat_id': None}),
        dcc.Store(id='current-selected-model-store', data=models()[0]['value']), # Store for selected model value
        html.Div(id='dummy-output', style={'display': 'none'}),
        html.Div( # Main Flex Container (Panel + Chat)
            [
                html.Div( # Side Panel Div
                    [
                        html.Button(id='panel-toggle-button', children=[DashIconify(icon="mdi:menu", width=24)], style=panel_toggle_button_style, n_clicks=0),
                        html.Div(
                            [
                                html.Button("New Chat", id='new-chat-button', style=new_chat_button_style, n_clicks=0),
                                html.Hr(style={'marginTop': '20px', 'marginBottom': '10px', 'borderColor': '#ddd'}),
                                html.H5("Previous Chats", style={'marginLeft': '5px', 'marginBottom': '5px', 'fontSize': '0.9rem', 'color': '#555'}),
                                html.Div(id='previous-chats-container', children=[], style=previous_chats_container_style)
                            ],
                            id='side-panel-content',
                            style=side_panel_content_closed_style
                        )
                    ],
                    id='side-panel',
                    style=side_panel_closed_style
                ),
                html.Div( # Chat Container Div
                    [
                        html.Div( # Inner container for chat interface
                           [
                                html.Div(id='submitted-text-display', style=submitted_display_style, children=[]),
                                html.Div([ # Input area wrapper
                                    dcc.Textarea(id='my-textarea', value='', style={'width': '100%', 'minHeight': f'{initial_height_px}px', 'maxHeight': f'{max_height_px}px', 'borderRadius': '10px', 'padding': '1ch', 'border': '1px solid #ccc', 'display': 'block', 'lineHeight': '1.5', 'resize': 'none', 'overflowY': 'auto'}, placeholder='Ask me something'),
                                    html.Div(id='filename-row', children=[], style=filename_row_style_hidden),
                                    html.Div(id='icon-row', children=[
                                        dcc.Upload(id='upload-data', children=html.Div([DashIconify(icon="mdi:paperclip", width=24, color="#555")]), style=upload_component_style, multiple=True, accept=','.join(ALLOWED_EXTENSIONS_UPLOAD)),
                                        html.Div(style={'flexGrow': 1}), # Spacer
                                        dbc.DropdownMenu(
                                            id='model-select-dropdown-button', # ID for the button part to update label
                                            label=models()[0]['value'], # Initial label
                                            children=[
                                                dbc.DropdownMenuItem(
                                                    item['label'],
                                                    id={'type': 'model-select-item', 'value': item['value']},
                                                    n_clicks=0 # Needed for input trigger
                                                ) for item in models()
                                            ],
                                            direction="up", # Open upwards
                                            color="secondary", # Example color, adjust as needed
                                            size="sm", # Smaller size to fit better
                                            style=dbc_dropdown_style
                                        ),
                                        html.Button(id='submit-button', children=[DashIconify(icon="mdi:send", width=24)], style=submit_button_style, n_clicks=0)
                                    ], style=icon_row_style_base),
                                ], style={'width': '100%', 'flexShrink': 0})
                           ],
                           style=chat_interface_style
                        )
                    ],
                    id='chat-container',
                    style=chat_container_panel_closed_style
                )
            ],
            style={ 'display': 'flex', 'flexDirection': 'row', 'alignItems': 'stretch', 'minHeight': '100vh', 'width': '100%', 'overflow': 'hidden'}
        )
    ]
)

# --- Callbacks ---

# ** Callback 0: Toggle Side Panel **
@callback(
    Output('side-panel', 'style', allow_duplicate=True),
    Output('side-panel-content', 'style', allow_duplicate=True),
    Output('panel-toggle-button', 'children', allow_duplicate=True),
    Output('side-panel-state-store', 'data', allow_duplicate=True),
    Input('panel-toggle-button', 'n_clicks'),
    State('side-panel-state-store', 'data'),
    prevent_initial_call=True
)
def toggle_side_panel(n_clicks, current_state):
    # ... (code unchanged) ...
    if n_clicks is None: return no_update, no_update, no_update, no_update
    is_open = current_state.get('isOpen', False)
    new_state = not is_open
    if new_state:
        panel_style, content_style, toggle_icon = side_panel_open_style, side_panel_content_open_style, DashIconify(icon="mdi:chevron-left", width=24)
    else:
        panel_style, content_style, toggle_icon = side_panel_closed_style, side_panel_content_closed_style, DashIconify(icon="mdi:menu", width=24)
    return panel_style, content_style, toggle_icon, {'isOpen': new_state}

# ** Callback: Start New Chat **
@callback(
    Output('message-history-store', 'data', allow_duplicate=True),
    Output('my-textarea', 'value', allow_duplicate=True),
    Output('uploaded-files-store', 'data', allow_duplicate=True),
    Output('current-chat-id-store', 'data', allow_duplicate=True),
    Output('side-panel-state-store', 'data', allow_duplicate=True),
    Output('side-panel', 'style', allow_duplicate=True),
    Output('side-panel-content', 'style', allow_duplicate=True),
    Output('panel-toggle-button', 'children', allow_duplicate=True),
    Input('new-chat-button', 'n_clicks'),
    prevent_initial_call='initial_duplicate'
)
def start_new_chat(n_clicks):
    # ... (code unchanged) ...
    if n_clicks is None or n_clicks < 1:
        return (no_update,) * 8
    print("\n--- Start New Chat Callback Triggered ---")
    new_chat_id = str(uuid.uuid4())
    print(f"Generated New Chat ID: {new_chat_id}")
    cleared_history = []
    cleared_textarea = ""
    cleared_files = []
    closed_panel_state = {'isOpen': False}
    closed_panel_style_val = side_panel_closed_style
    closed_content_style_val = side_panel_content_closed_style
    closed_toggle_icon = DashIconify(icon="mdi:menu", width=24)
    return (
        cleared_history, cleared_textarea, cleared_files, new_chat_id,
        closed_panel_state, closed_panel_style_val, closed_content_style_val, closed_toggle_icon
    )

# ** Callback: Render Previous Chats List **
@callback(
    Output('previous-chats-container', 'children'),
    Input('previous-chats-store', 'data'),
    Input('editing-summary-state-store', 'data')
)
def render_previous_chats(previous_chats_data, editing_state):
    # ... (code unchanged - uses trash can icon) ...
    if not previous_chats_data:
        return html.P("No previous chats yet.", style={'padding': '8px 5px', 'color': '#888', 'fontSize': '0.9em'})
    editing_chat_id = editing_state.get('editing_chat_id') if editing_state else None
    chat_elements = []
    for chat_data in previous_chats_data:
        summary = chat_data.get('summary', 'Chat (no summary)')
        chat_id = chat_data.get('id')
        if not chat_id: continue
        item_content = []
        if chat_id == editing_chat_id:
            item_content.append(dcc.Input(id={'type': 'edit-summary-input', 'chat_id': chat_id}, value=summary, style=edit_summary_input_style, debounce=False, size=str(len(summary) + 2)))
            item_content.append(html.Button(DashIconify(icon="mdi:check", width=16), id={'type': 'save-summary-button', 'chat_id': chat_id}, style=previous_chat_action_button_style, title="Save", n_clicks=0))
            item_content.append(html.Button(DashIconify(icon="mdi:close", width=16), id={'type': 'cancel-edit-summary-button', 'chat_id': chat_id}, style=previous_chat_action_button_style, title="Cancel", n_clicks=0))
        else:
            item_content.append(html.Button(summary, id={'type': 'load-chat-button', 'chat_id': chat_id}, style=previous_chat_button_style, title=summary, n_clicks=0))
            item_content.append(html.Button(DashIconify(icon="mdi:pencil-outline", width=16), id={'type': 'edit-summary-button', 'chat_id': chat_id}, style=previous_chat_action_button_style, title="Edit name", n_clicks=0))
            item_content.append(html.Button(DashIconify(icon="mdi:trash-can-outline", width=16), id={'type': 'delete-previous-chat', 'chat_id': chat_id}, style=previous_chat_action_button_style, title="Delete this chat", n_clicks=0))
        item_container = html.Div(item_content, style=previous_chat_item_container_style, key=chat_id)
        chat_elements.append(item_container)
    return chat_elements

# ** Callback: Load Previous Chat **
@callback(
    Output('message-history-store', 'data', allow_duplicate=True),
    Output('my-textarea', 'value', allow_duplicate=True),
    Output('uploaded-files-store', 'data', allow_duplicate=True),
    Output('current-chat-id-store', 'data', allow_duplicate=True),
    Output('previous-chats-store', 'data', allow_duplicate=True), # Add output back
    Output('side-panel-state-store', 'data', allow_duplicate=True),
    Output('side-panel', 'style', allow_duplicate=True),
    Output('side-panel-content', 'style', allow_duplicate=True),
    Output('panel-toggle-button', 'children', allow_duplicate=True),
    Input({'type': 'load-chat-button', 'chat_id': ALL}, 'n_clicks'),
    State('previous-chats-store', 'data'),
    State('message-history-store', 'data'), # Add state back
    State('current-chat-id-store', 'data'), # Add state back
    prevent_initial_call='initial_duplicate'
)
def load_previous_chat(n_clicks, previous_chats_list, current_chat_history, 
                       current_chat_id):
    # ... (code unchanged - already moves loaded chat to top) ...
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict) or triggered.get('type') != 'load-chat-button' or not any(n > 0 for n in n_clicks if n is not None):
        return (no_update,) * 9
    chat_id_to_load = triggered['chat_id']
    print(f"\n--- Load Previous Chat Callback Triggered (ID: {chat_id_to_load}) ---")
    if not previous_chats_list: previous_chats_list = []
    updated_previous_chats = list(previous_chats_list)
    current_chat_entry = None
    found_index_for_current = -1
    if current_chat_history and current_chat_id:
        try:
            first_message = current_chat_history[0]
            first_question_text = first_message.get('text', '')
            current_summary = get_summary(first_question_text)
            current_chat_entry = {'id': current_chat_id, 'summary': current_summary, 'history': current_chat_history}
            print(f"Attempting to save current chat (ID: {current_chat_id}), summary: {current_summary}")
            for i, chat in enumerate(updated_previous_chats):
                if chat.get('id') == current_chat_id:
                    found_index_for_current = i
                    break
            if found_index_for_current != -1:
                 updated_previous_chats[found_index_for_current] = current_chat_entry
                 print(f"Updated existing entry for current chat at index {found_index_for_current}.")
            # else: # Don't insert here, handle after moving loaded chat
        except Exception as e:
            print(f"Error during current chat saving logic in load_previous_chat: {e}")
            updated_previous_chats = no_update
    else:
        print("Current chat history or ID is empty/None, nothing to save.")
    history_to_load = None
    summary_loaded = "Not Found"
    chat_to_move = None
    index_to_load = -1
    if isinstance(updated_previous_chats, list):
        for i, chat_entry in enumerate(updated_previous_chats):
            if chat_entry.get('id') == chat_id_to_load:
                index_to_load = i
                break
        if index_to_load != -1:
            chat_to_move = updated_previous_chats.pop(index_to_load)
            history_to_load = chat_to_move.get('history', [])
            summary_loaded = chat_to_move.get('summary', 'N/A')
            if current_chat_entry and found_index_for_current == -1:
                 updated_previous_chats.insert(0, current_chat_entry)
                 print("Inserted current chat as new entry at index 0.")
            updated_previous_chats.insert(0, chat_to_move)
            print(f"Moved loaded chat '{summary_loaded}' to top.")
        else:
             print(f"Error: Chat with ID {chat_id_to_load} not found after potential save.")
             updated_previous_chats = no_update
             if history_to_load is None: return (no_update,) * 9
    else:
        print("Skipping loading because previous chats list wasn't updated due to error.")
        return (no_update,) * 9
    cleared_textarea = ""
    cleared_files = []
    closed_panel_state = {'isOpen': False}
    closed_panel_style_val = side_panel_closed_style
    closed_content_style_val = side_panel_content_closed_style
    closed_toggle_icon = DashIconify(icon="mdi:menu", width=24)
    print(f"Final Previous Chats List (Load - to be returned): {[chat.get('summary', 'ERR') for chat in updated_previous_chats if isinstance(updated_previous_chats, list)]}")
    return (
        history_to_load, cleared_textarea, cleared_files, chat_id_to_load,
        updated_previous_chats, # Return the reordered list
        closed_panel_state, closed_panel_style_val, closed_content_style_val, closed_toggle_icon
    )


# ** Callback: Delete Previous Chat **
@callback(
    Output('previous-chats-store', 'data', allow_duplicate=True),
    Input({'type': 'delete-previous-chat', 'chat_id': ALL}, 'n_clicks'),
    State('previous-chats-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def delete_previous_chat(n_clicks, previous_chats_list):
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict) or triggered.get('type') != 'delete-previous-chat' or not any(n > 0 for n in n_clicks if n is not None):
        return no_update
    chat_id_to_delete = triggered['chat_id']
    print(f"\n--- Delete Previous Chat Callback Triggered (ID: {chat_id_to_delete}) ---")
    if not previous_chats_list:
        return no_update
    updated_previous_chats = [ chat for chat in previous_chats_list if chat.get('id') != chat_id_to_delete ]
    if len(updated_previous_chats) == len(previous_chats_list):
        print(f"Chat ID {chat_id_to_delete} not found for deletion.")
        return no_update
    
    _chat = bedrock.Chat(chat_id_to_delete).clear_context()

    print(f"Deleted chat with ID: {chat_id_to_delete}")
    return updated_previous_chats

# ** Callback: Start Editing Previous Chat Summary **
@callback(
    Output('editing-summary-state-store', 'data'),
    Input({'type': 'edit-summary-button', 'chat_id': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def start_editing_summary(n_clicks):
    # ... (code unchanged) ...
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict) or triggered.get('type') != 'edit-summary-button' or not any(n > 0 for n in n_clicks if n is not None):
        return no_update
    chat_id_to_edit = triggered['chat_id']
    print(f"Starting edit for chat ID: {chat_id_to_edit}")
    return {'editing_chat_id': chat_id_to_edit}

# ** Callback: Cancel Editing Previous Chat Summary **
@callback(
    Output('editing-summary-state-store', 'data', allow_duplicate=True),
    Input({'type': 'cancel-edit-summary-button', 'chat_id': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def cancel_summary_edit(n_clicks):
    # ... (code unchanged) ...
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict) or triggered.get('type') != 'cancel-edit-summary-button' or not any(n > 0 for n in n_clicks if n is not None):
        return no_update
    print("Canceling summary edit.")
    return {'editing_chat_id': None}

# ** Callback: Save Edited Previous Chat Summary **
@callback(
    Output('previous-chats-store', 'data', allow_duplicate=True),
    Output('editing-summary-state-store', 'data', allow_duplicate=True),
    Input({'type': 'save-summary-button', 'chat_id': ALL}, 'n_clicks'),
    Input({'type': 'edit-summary-input', 'chat_id': ALL}, 'n_submit'),
    State({'type': 'edit-summary-input', 'chat_id': ALL}, 'value'),
    State({'type': 'edit-summary-input', 'chat_id': ALL}, 'id'),
    State('previous-chats-store', 'data'),
    prevent_initial_call=True
)
def save_summary_edit(save_n_clicks, input_n_submit, input_values, input_ids, previous_chats_list):
    # ... (code unchanged) ...
    triggered_id_obj = ctx.triggered_id
    if isinstance(triggered_id_obj, dict) and triggered_id_obj.get('type') in ['save-summary-button', 'edit-summary-input']:
         chat_id_to_save = triggered_id_obj['chat_id']
    else:
        print("Save summary triggered by unexpected ID:", triggered_id_obj)
        return no_update, no_update
    trigger_valid = False
    if triggered_id_obj.get('type') == 'save-summary-button':
        trigger_index = next((i for i, d in enumerate(ctx.inputs_list[0]) if d['id'] == triggered_id_obj), -1)
        if trigger_index != -1 and save_n_clicks[trigger_index] and save_n_clicks[trigger_index] >= 1: trigger_valid = True
    elif triggered_id_obj.get('type') == 'edit-summary-input':
         trigger_index = next((i for i, d in enumerate(ctx.inputs_list[1]) if d['id'] == triggered_id_obj), -1)
         if trigger_index != -1 and input_n_submit[trigger_index] and input_n_submit[trigger_index] >= 1: trigger_valid = True
    if not trigger_valid: return no_update, no_update
    print(f"Saving summary for chat ID: {chat_id_to_save}")
    if not previous_chats_list:
        print("Error: Previous chats list is empty, cannot save.")
        return no_update, {'editing_chat_id': None}
    new_summary = None
    if input_values and input_ids:
        for i, input_id_dict in enumerate(input_ids):
            if isinstance(input_id_dict, dict) and input_id_dict.get('chat_id') == chat_id_to_save:
                new_summary = input_values[i]
                break
    if new_summary is None:
        print("Error: Could not find input value for the chat being saved.")
        return no_update, {'editing_chat_id': None}
    updated_previous_chats = []
    found = False
    for chat in previous_chats_list:
        if chat.get('id') == chat_id_to_save:
            updated_chat = {**chat, 'summary': new_summary.strip()}
            updated_previous_chats.append(updated_chat)
            found = True
            print(f"Updated summary for {chat_id_to_save} to: {new_summary.strip()}")
        else:
            updated_previous_chats.append(chat)
    if not found:
         print(f"Error: Chat ID {chat_id_to_save} not found when saving summary.")
         return no_update, {'editing_chat_id': None}
    return updated_previous_chats, {'editing_chat_id': None}


# --- Remaining Callbacks (Unchanged) ---

# **Callback 1: Handle new file uploads**
@callback(
    Output('uploaded-files-store', 'data', allow_duplicate=True),
    Input('upload-data', 'contents'),
    Input('upload-data', 'filename'),
    State('uploaded-files-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def handle_uploads(list_of_contents, list_of_names, current_files):
    """
    Adds newly uploaded files to the temporary store.
    If a zip file is uploaded, extracts filenames with allowed extensions.
    """
    print("\n--- handle_uploads triggered ---") # DEBUG
    print(f"Input filenames: {list_of_names}") # DEBUG
    print(f"Current files in store (state): {current_files}") # DEBUG

    if list_of_contents is None or list_of_names is None:
        print("handle_uploads: No contents or names provided.") # DEBUG
        return no_update

    if current_files is None:
        current_files = []

    updated_files_list = copy.deepcopy(current_files)
    # Get filenames already in the list *before* this batch
    current_filenames_in_store = {f['filename'] for f in updated_files_list}
    files_added_this_run = 0

    for content, name in zip(list_of_contents, list_of_names):
        if name is None or content is None:
            continue

        file_lower = name.lower()
        # Check if it's a zip file
        if file_lower.endswith('.zip'):
            try:
                content_type, content_string = content.split(',')
                decoded = base64.b64decode(content_string)
                zip_str = io.BytesIO(decoded)

                with zipfile.ZipFile(zip_str, 'r') as zip_ref:
                    for member_name in zip_ref.namelist():
                        if member_name.endswith('/') or member_name.startswith('__MACOSX/') or os.path.basename(member_name).startswith('.'):
                            continue
                        _, ext = os.path.splitext(member_name)
                        if ext.lower() in ALLOWED_EXTENSIONS:
                            qualified_name = f"{name}/{member_name}"
                            # Check against files already present (before this batch + added in this batch)
                            if not any(f['filename'] == qualified_name for f in updated_files_list):
                                file_id = str(uuid.uuid4())
                                updated_files_list.append({'filename': qualified_name, 'id': file_id})
                                files_added_this_run += 1
                                print(f"Staged from zip: {qualified_name}")
                            else:
                                 print(f"Skipping duplicate file from zip: {qualified_name}") # DEBUG

            except zipfile.BadZipFile: print(f"Error: Uploaded file '{name}' is not a valid zip file.")
            except Exception as e: print(f"Error processing zip file '{name}': {e}")

        else:
            # Handle regular file uploads
             _, ext = os.path.splitext(name)
             if ext.lower() in ALLOWED_EXTENSIONS_UPLOAD:
                 # Check against files already present (before this batch + added in this batch)
                 if not any(f['filename'] == name for f in updated_files_list):
                    file_id = str(uuid.uuid4())
                    updated_files_list.append({'filename': name, 'id': file_id})
                    files_added_this_run += 1
                    print(f"Staged file: {name}")
                 else:
                     print(f"Skipping duplicate file: {name}") # DEBUG
             else:
                 print(f"Skipping file with disallowed extension: {name}") # DEBUG

    print(f"Files added in this run: {files_added_this_run}") # DEBUG
    print(f"Returning updated file list: {updated_files_list}") # DEBUG

    return updated_files_list


# **Callback 2: Render currently attached file list**
@callback(
    Output('filename-row', 'children'),
    Input('uploaded-files-store', 'data')
)
def render_current_file_list(stored_files):
    # ... (code unchanged) ...
    if not stored_files: return []
    file_items = []
    for file_info in stored_files:
        file_id = file_info['id']
        filename = file_info['filename']
        item = html.Div([html.Span(filename, style=filename_text_style), html.Button(id={'type': 'delete-file-button', 'index': file_id}, children=[DashIconify(icon="mdi:trash-can-outline", width=16, style={'display': 'block'})], style=action_button_style, n_clicks=0, title=f"Remove {filename}")], style=file_item_style, key=file_id)
        file_items.append(item)
    return file_items

# **Callback 3: Handle deletion of a current file**
@callback(
    Output('uploaded-files-store', 'data', allow_duplicate=True),
    Input({'type': 'delete-file-button', 'index': ALL}, 'n_clicks'),
    State('uploaded-files-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def handle_current_file_delete(n_clicks, current_files):
    # ... (code unchanged - already prints filename) ...
    if not ctx.triggered_id or not any(n > 0 for n in n_clicks if n is not None): return no_update
    button_id = ctx.triggered_id
    file_id_to_delete = button_id['index']
    if not current_files: return []
    filename_deleted = "Unknown"
    for f in current_files:
        if f['id'] == file_id_to_delete:
            filename_deleted = f['filename']
            break
    updated_files = [f for f in current_files if f['id'] != file_id_to_delete]
    print(f"Deleted staged file: {filename_deleted}")
    return updated_files

# **Callback 4: Update visibility/spacing of filename row and icon row**
@callback(
    Output('filename-row', 'style'),
    Output('icon-row', 'style'),
    Input('uploaded-files-store', 'data')
)
def update_current_file_spacing(stored_files):
    # ... (code unchanged) ...
    if not stored_files:
        return filename_row_style_hidden, icon_row_style_base
    else:
        return filename_row_style_visible, icon_row_style_no_margin

# **Callback 5: Handle Submit button click**
@callback(
    Output('message-history-store', 'data', allow_duplicate=True),
    Output('my-textarea', 'value', allow_duplicate=True),
    Output('uploaded-files-store', 'data', allow_duplicate=True),
    Output('previous-chats-store', 'data', allow_duplicate=True),
    Output('current-chat-id-store', 'data', allow_duplicate=True),
    Input('submit-button', 'n_clicks'),
    State('my-textarea', 'value'),
    State('message-history-store', 'data'),
    State('current-selected-model-store', 'data'),
    State('uploaded-files-store', 'data'),
    State('previous-chats-store', 'data'),
    State('current-chat-id-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def handle_submit(n_clicks, text_value, current_history, selected_model, 
                  current_uploaded_files, previous_chats_list, current_chat_id):
    # ... (code unchanged - already updates previous chats on every submit) ...

    _chat = bedrock.Chat(current_chat_id)

    processed_text_value = text_value.strip() if text_value else ""
    previous_chats_output = no_update
    current_chat_id_output = no_update
    if processed_text_value or current_uploaded_files:
        if current_history is None: current_history = []
        if current_uploaded_files is None: current_uploaded_files = []
        associated_filenames = [f['filename'] for f in current_uploaded_files]
        answer = _chat.ask(processed_text_value)
        answer_text = answer["text"]
        if associated_filenames: answer_text += f" (Files: {', '.join(associated_filenames)})"
        new_entry = {
            'text': processed_text_value, 
            'files': associated_filenames, 
            'answer': answer_text,
            'cost': answer["cost"],
            'time': answer["time"],
            }
        updated_history = current_history + [new_entry]
        print(f"Submit Question: '{processed_text_value}', Files: {associated_filenames}, Model: '{selected_model}'")
        if updated_history:
            if current_chat_id is None:
                current_chat_id = str(uuid.uuid4())
                current_chat_id_output = current_chat_id
                print(f"Generated initial Chat ID: {current_chat_id}")
            updated_previous_chats = previous_chats_list if previous_chats_list is not None else []
            summary = get_summary(updated_history[0].get('text', ''))
            chat_entry = {'id': current_chat_id, 'summary': summary, 'history': updated_history}
            found_index = -1
            for i, chat in enumerate(updated_previous_chats):
                if chat.get('id') == current_chat_id:
                    found_index = i
                    break
            if found_index != -1:
                updated_previous_chats[found_index] = chat_entry
                print(f"Updated chat {current_chat_id} in previous list.")
            else:
                updated_previous_chats.insert(0, chat_entry)
                print(f"Inserted new chat {current_chat_id} into previous list.")
            previous_chats_output = updated_previous_chats
        return updated_history, "", [], previous_chats_output, current_chat_id_output
    else:
        print("Submit: No text or files.")
        return no_update, no_update, no_update, no_update, no_update


# **Callback 6: Render message history**
@callback(
    Output('submitted-text-display', 'children'),
    Input('message-history-store', 'data'),
    State('current-chat-id-store', 'data'),
)
def render_message_history(message_list, current_chat_id):

    _chat = bedrock.Chat(current_chat_id)

    # ... (code unchanged) ...
    if not message_list: return []
    message_elements = []
    num_messages = len(message_list)
    for index, entry in enumerate(message_list):
        is_last_message = (index == num_messages - 1)
        msg = entry.get('text', '')
        files = entry.get('files', [])
        answer = entry.get('answer', '')
        model_used = entry.get('model_used', 'N/A') # Get the model name
        q_icon_div = html.Div("Q", style=q_box_style)
        question_box_content = [dcc.Markdown(msg)]
        if is_last_message:
            edit_btn = html.Button(id={'type': 'edit-message-button', 'index': index}, children=[DashIconify(icon="mdi:pencil-outline", width=16)], style=action_icon_button_style, n_clicks=0, title="Edit this message")
            delete_btn = html.Button(id={'type': 'delete-message-button', 'index': index}, children=[DashIconify(icon="mdi:trash-can-outline", width=16)], style=action_icon_button_style, n_clicks=0, title="Delete this message")
            button_container = html.Div([edit_btn, delete_btn], style=action_button_container_style)
            question_box_content.append(button_container)
        question_box_div = html.Div(question_box_content, style=question_box_style)
        qa_row = html.Div([q_icon_div, question_box_div], style=message_qa_row_style)
        files_row = None
        if files:
            file_spans = []
            for fname in files:
                if is_last_message:
                    file_spans.append(html.Div([html.Span(fname, style=deletable_filename_text_style), html.Button(id={'type': 'delete-history-file', 'index': index, 'filename': fname}, children=[DashIconify(icon="mdi:trash-can-outline", width=16, style={'display': 'block'})], style=history_delete_button_style, n_clicks=0, title=f"Remove {fname}")], style=deletable_file_item_style))
                else:
                    file_spans.append(html.Span(fname, style=message_filename_style))
            files_row = html.Div(file_spans, style=message_files_style)
        answer_row = None
        if answer:
            a_icon_div = html.Div("A", style=a_box_style)
            current_model = _chat.list_models()[0]
            context_length = _chat.context_length()
            cost = entry.get('cost', 0)
            time = entry.get('time', 0)
            meta = f"model: {current_model}, context length: {context_length}, cost: ${cost:.3f}, time: {time:.1f}s"
            answer_box_div = html.Div([
                dcc.Markdown(answer, id=f"answer-text-{index}", style={'margin': '0', 'padding': '0'}), 
                html.Div(meta, style=model_display_style), # Display the model name
                html.Button(id={'type': 'copy-answer-button', 'index': index}, children=[DashIconify(icon="mdi:content-copy", width=16)], style=bottom_right_button_style, n_clicks=0, title="Copy answer to clipboard")], style=answer_box_style)
            answer_row = html.Div([a_icon_div, answer_box_div], style={**message_qa_row_style, 'marginTop': '0.5em'})
        message_entry_children = [qa_row]
        if files_row: message_entry_children.append(files_row)
        if answer_row: message_entry_children.append(answer_row)
        message_entry = html.Div(message_entry_children, style=message_entry_style, key=f"msg-{index}")
        message_elements.append(message_entry)
    return message_elements

# **Callback 7: Handle file deletion from last message**
@callback(
    Output('message-history-store', 'data', allow_duplicate=True),
    Input({'type': 'delete-history-file', 'index': ALL, 'filename': ALL}, 'n_clicks'),
    State('message-history-store', 'data'),
    State('current-selected-model-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def handle_history_file_delete(n_clicks, current_history, selected_model):
    # ... (code unchanged - already prints filename) ...
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict) or triggered.get('type') != 'delete-history-file' or not any(n > 0 for n in n_clicks if n is not None): return no_update
    msg_index = triggered['index']
    filename_to_delete = triggered['filename']
    if not current_history or msg_index != len(current_history) - 1: return no_update
    last_entry = current_history[-1]
    original_text = last_entry.get('text', '')
    original_files = last_entry.get('files', [])
    remaining_files = [f for f in original_files if f != filename_to_delete]
    new_answer_text = f"Re-answered '{original_text}' using {selected_model}."
    if remaining_files: new_answer_text += f" (Files: {', '.join(remaining_files)})"
    else: new_answer_text += " (No files remaining)"
    current_history[-1]['files'] = remaining_files
    current_history[-1]['answer'] = new_answer_text
    print(f"Deleted history file '{filename_to_delete}', re-answered.")
    return current_history

# **Callback 8: Handle deletion of the entire last message**
@callback(
    Output('message-history-store', 'data', allow_duplicate=True),
    Input({'type': 'delete-message-button', 'index': ALL}, 'n_clicks'),
    State('message-history-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def handle_message_delete(n_clicks, current_history):
    # ... (code unchanged - already prints text) ...
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict) or triggered.get('type') != 'delete-message-button' or not any(n > 0 for n in n_clicks if n is not None): return no_update
    msg_index = triggered['index']
    if not current_history or msg_index != len(current_history) - 1: return no_update
    text_deleted = current_history[-1].get('text', '[empty question]')
    print(f"Deleting last message (index {msg_index}): '{text_deleted}'")
    updated_history = current_history[:-1]
    return updated_history

# **Callback 9: Handle Edit button click**
@callback(
    Output('my-textarea', 'value', allow_duplicate=True),
    Output('uploaded-files-store', 'data', allow_duplicate=True),
    Output('message-history-store', 'data', allow_duplicate=True),
    Input({'type': 'edit-message-button', 'index': ALL}, 'n_clicks'),
    State('message-history-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def handle_edit_message(n_clicks, current_history):
    # ... (code unchanged - already prints text) ...
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict) or triggered.get('type') != 'edit-message-button' or not any(n > 0 for n in n_clicks if n is not None): return no_update, no_update, no_update
    msg_index = triggered['index']
    if not current_history or msg_index != len(current_history) - 1: return no_update, no_update, no_update
    last_entry = current_history[-1]
    text_to_edit = last_entry.get('text', '')
    files_to_edit_names = last_entry.get('files', [])
    files_for_staging = [{'filename': name, 'id': str(uuid.uuid4())} for name in files_to_edit_names]
    updated_history = current_history[:-1]
    print(f"Editing message index {msg_index}: '{text_to_edit}'")
    return text_to_edit, files_for_staging, updated_history


# **Callback 10: Example callback for dropdown change**
@callback(
    Output('dummy-output', 'children', allow_duplicate=True),
    Input('current-selected-model-store', 'data'), # Triggered by store change
    State('current-chat-id-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def update_output_div(selected_value, current_chat_id):
    # ... (code unchanged) ...
    print(f"Dropdown value changed to: {selected_value}")
    _chat = bedrock.Chat(current_chat_id)
    if selected_value == "auto":
        _chat.reset_model()
    else:
        _chat.set_model(selected_value)
    return None

# ** NEW Callback: Handle Model Selection **
@callback(
    Output('current-selected-model-store', 'data'),
    Output('model-select-dropdown-button', 'label'), # Update button label
    Input({'type': 'model-select-item', 'value': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def update_model_selection(n_clicks):
    """Updates the selected model based on dropdown item clicks."""
    triggered_id = ctx.triggered_id
    if not triggered_id or not isinstance(triggered_id, dict) or not any(n > 0 for n in n_clicks if n is not None):
        return no_update, no_update

    selected_value = triggered_id['value']
    selected_label = next((item['label'] for item in models() if item['value'] == selected_value), "Select Model")

    print(f"Model selection updated to: {selected_value}")
    return selected_value, selected_label

# **Clientside Callback: Adjust textarea height**
clientside_callback(
    """
    function(value, currentStyle) {
        // ... (JavaScript code unchanged) ...
        if (!window.dash_clientside || !window.dash_clientside.no_update) { console.warn("Dash clientside not ready for resize."); return currentStyle || {}; }
        const el = document.getElementById('my-textarea'); const safeCurrentStyle = currentStyle || {};
        if (!el || !el.style) { console.warn("Textarea not found for resize."); return window.dash_clientside.no_update; }
        const minHeightPx = """ + str(initial_height_px) + """; const maxHeightPx = """ + str(max_height_px) + """;
        const originalHeight = el.style.height;
        try {
            el.style.height = 'auto'; const scrollHeight = el.scrollHeight; el.style.height = originalHeight;
            const computedStyle = window.getComputedStyle(el); if (!computedStyle) { console.error("No computed style"); return window.dash_clientside.no_update; }
            const paddingTop = parseFloat(computedStyle.paddingTop) || 0; const paddingBottom = parseFloat(computedStyle.paddingBottom) || 0;
            const borderTop = parseFloat(computedStyle.borderTopWidth) || 0; const borderBottom = parseFloat(computedStyle.borderBottomWidth) || 0;
            if (isNaN(paddingTop) || isNaN(paddingBottom) || isNaN(borderTop) || isNaN(borderBottom)) { console.error("NaN styles"); return window.dash_clientside.no_update; }
            const requiredTotalHeight = scrollHeight + borderTop + borderBottom + 2;
            const minTotalHeight = minHeightPx + paddingTop + paddingBottom + borderTop + borderBottom;
            let newTotalHeightPx = Math.max(minTotalHeight, requiredTotalHeight); newTotalHeightPx = Math.min(maxHeightPx, newTotalHeightPx);
            const newContentHeightPx = newTotalHeightPx - paddingTop - paddingBottom - borderTop - borderBottom;
            const newStyle = {...safeCurrentStyle}; newStyle.height = newContentHeightPx + 'px';
            const maxContentHeight = maxHeightPx - paddingTop - paddingBottom - borderTop - borderBottom;
            newStyle.overflowY = (scrollHeight > maxContentHeight) ? 'auto' : 'hidden';
            if (newStyle.height !== safeCurrentStyle.height || newStyle.overflowY !== safeCurrentStyle.overflowY) { return newStyle; } else { return window.dash_clientside.no_update; }
        } catch (e) { console.error("Textarea height error:", e); return window.dash_clientside.no_update; }
    }
    """,
    Output('my-textarea', 'style'), Input('my-textarea', 'value'), State('my-textarea', 'style')
)

# **Clientside Callback: Copy-to-clipboard functionality**
clientside_callback(
    """
    function(n_clicks_list) {
        // ... (JavaScript code unchanged) ...
        if (!window.dash_clientside || !window.dash_clientside.no_update || !window.dash_clientside.callback_context) { console.warn("Dash clientside not ready for copy."); return window.dash_clientside && window.dash_clientside.no_update ? window.dash_clientside.no_update : null; }
        const triggered = window.dash_clientside.callback_context.triggered;
        if (!triggered || triggered.length === 0 || !triggered[0].prop_id || !triggered[0].prop_id.includes('.n_clicks')) { return window.dash_clientside.no_update; }
        if (!navigator.clipboard || !navigator.clipboard.writeText) { console.error("Clipboard API not available."); return window.dash_clientside.no_update; }
        const triggeredIdStr = triggered[0].prop_id.split('.')[0];
        try {
            const idObj = JSON.parse(triggeredIdStr); if (!idObj || idObj.type !== 'copy-answer-button' || typeof idObj.index === 'undefined') { return window.dash_clientside.no_update; }
            const index = idObj.index; const answerElement = document.querySelector(`#answer-text-${index}`);
            if (!answerElement) { console.error(`Answer element #${index} not found.`); return window.dash_clientside.no_update; }
            const textToCopy = answerElement.innerText || answerElement.textContent;
            navigator.clipboard.writeText(textToCopy).then(() => { console.log('Answer copied.'); }).catch(err => { console.error('Copy failed: ', err); });
        } catch (e) { if (triggeredIdStr.includes('copy-answer-button')) { console.error("Copy callback error:", e); } }
        return window.dash_clientside.no_update;
    }
    """,
    Output('dummy-output', 'children', allow_duplicate=True), Input({'type': 'copy-answer-button', 'index': ALL}, 'n_clicks'), prevent_initial_call='initial_duplicate'
)

# ** Clientside Callback: Scroll chat history to bottom **
clientside_callback(
    """
    function(children) {
        // Add slight delay to allow DOM to update if necessary
        setTimeout(function() {
            const element = document.getElementById('submitted-text-display');
            if (element) {
                // console.log("Scrolling chat history to bottom"); // Optional debug
                element.scrollTop = element.scrollHeight;
            } else {
                console.warn("Chat history element 'submitted-text-display' not found for scrolling.");
            }
        }, 50); // 50ms delay, adjust if needed

        // This callback doesn't need to return anything to Dash
        return window.dash_clientside.no_update;
    }
    """,
    Output('dummy-output', 'children', allow_duplicate=True), # Use dummy output
    Input('submitted-text-display', 'children'),
    prevent_initial_call=True # Don't scroll on initial load
)

clientside_callback(
    """
    function(contents) {
        // Only reset if contents are not null/undefined (i.e., an upload just happened)
        if (!contents || !window.dash_clientside) {
            return window.dash_clientside.no_update;
        }
        try {
            // Find the actual file input within the dcc.Upload component
            var inputElement = document.getElementById('upload-data').querySelector('input[type="file"]');
            if (inputElement) {
                // console.log("Resetting upload input value"); // DEBUG
                inputElement.value = null; // Reset the file input value
            } else {
                 console.warn("Could not find input element within upload-data to reset.");
            }
        } catch (e) {
            console.error("Error resetting upload input:", e);
        }
        return window.dash_clientside.no_update; // No Dash output needs updating
    }
    """,
    Output('dummy-output', 'children', allow_duplicate=True), # Use dummy output
    Input('upload-data', 'contents'), # Trigger when new content arrives
    prevent_initial_call=True # Don't run on initial load
)

clientside_callback(
    """
    function(_) { // We don't need the keydown event data itself, just the trigger
        // Get the textarea element
        const textarea = document.getElementById('my-textarea');
        if (!textarea) return window.dash_clientside.no_update; // Exit if textarea not found

        // Define the event handler function
        const handleEnter = (event) => {
            // Check if Enter key was pressed WITHOUT the Shift key
            if (event.key === 'Enter' && !event.shiftKey) {
                // console.log("Enter pressed without Shift."); // Debug

                // Prevent the default action (adding a new line)
                event.preventDefault();

                // Find the submit button
                const submitButton = document.getElementById('submit-button');

                if (submitButton) {
                    // console.log("Clicking submit button programmatically."); // Debug
                    // Programmatically click the submit button
                    submitButton.click();
                } else {
                    console.error("Submit button ('submit-button') not found.");
                }
            } else {
                // console.log(`Key pressed: ${event.key}, Shift: ${event.shiftKey}. Allowing default.`); // Debug
                // Allow default behavior for Shift+Enter or other keys
            }
        };

        // --- IMPORTANT ---
        // Remove existing listener before adding a new one to prevent duplicates
        // We need a way to reference the *exact same* handler function to remove it.
        // Store the handler on the element itself or use a more robust event management strategy
        // if this callback could potentially run multiple times in complex scenarios.
        // For simplicity here, we assume it runs once or that re-adding is handled okay by browser.
        // A more robust way:
        if (textarea._handleEnterListener) {
             textarea.removeEventListener('keydown', textarea._handleEnterListener);
        }
        textarea.addEventListener('keydown', handleEnter);
        textarea._handleEnterListener = handleEnter; // Store reference for potential removal


        // This setup callback doesn't update any Dash components directly
        return window.dash_clientside.no_update;
    }
    """,
    Output('dummy-output', 'children', allow_duplicate=True), # Output to dummy component
    # Trigger this setup once when the textarea's value changes (e.g., on load or clear)
    # Using 'value' ensures it runs after the element is likely in the DOM.
    Input('my-textarea', 'value'),
    prevent_initial_call='initial_duplicate' # Run on initial load/setup
)

# --- Run App ---
if __name__ == '__main__':
    app.run(debug=True)
