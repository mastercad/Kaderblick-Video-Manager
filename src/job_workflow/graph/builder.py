from __future__ import annotations


def build_default_graph(graph_view, draft) -> None:
    source_map = {
        "files": "source_files",
        "folder_scan": "source_folder_scan",
        "pi_download": "source_pi_download",
    }
    source_id = graph_view.add_node(source_map.get(draft.source_mode, "source_files"))
    previous_id = source_id
    if draft.convert_enabled:
        previous_id = graph_view.add_node("convert")
        graph_view.connect_nodes(source_id, previous_id)
    if any(file.merge_group_id for file in draft.files):
        merge_id = graph_view.add_node("merge")
        graph_view.connect_nodes(previous_id, merge_id)
        previous_id = merge_id
    if draft.title_card_enabled:
        title_id = graph_view.add_node("titlecard")
        graph_view.connect_nodes(previous_id, title_id)
        previous_id = title_id
    if draft.create_youtube_version:
        yt_version_id = graph_view.add_node("yt_version")
        graph_view.connect_nodes(previous_id, yt_version_id)
        previous_id = yt_version_id
    if draft.upload_youtube:
        upload_id = graph_view.add_node("youtube_upload")
        graph_view.connect_nodes(previous_id, upload_id)
        previous_id = upload_id
    if draft.upload_youtube and draft.upload_kaderblick:
        kb_id = graph_view.add_node("kaderblick")
        graph_view.connect_nodes(previous_id, kb_id)