import importlib.util


def test_reorganized_packages_are_importable_and_flat_modules_are_gone():
    workflow_spec = importlib.util.find_spec("src.workflow")
    settings_spec = importlib.util.find_spec("src.settings")
    app_spec = importlib.util.find_spec("src.app")

    assert workflow_spec is not None
    assert workflow_spec.submodule_search_locations is not None
    assert settings_spec is not None
    assert settings_spec.submodule_search_locations is not None
    assert app_spec is not None
    assert app_spec.submodule_search_locations is not None
    dialogs_spec = importlib.util.find_spec("src.ui.dialogs")
    job_editor_spec = importlib.util.find_spec("src.ui.job_editor")
    executor_spec = importlib.util.find_spec("src.runtime.workflow_executor")
    assert dialogs_spec is not None
    assert dialogs_spec.submodule_search_locations is not None
    assert job_editor_spec is not None
    assert job_editor_spec.submodule_search_locations is not None
    assert executor_spec is not None
    assert executor_spec.submodule_search_locations is not None
    assert importlib.util.find_spec("src.workflow.model") is not None
    assert importlib.util.find_spec("src.workflow.graph") is not None
    assert importlib.util.find_spec("src.workflow.storage") is not None
    assert importlib.util.find_spec("src.workflow.migration") is not None
    assert importlib.util.find_spec("src.settings.model") is not None
    assert importlib.util.find_spec("src.settings.io") is not None
    assert importlib.util.find_spec("src.settings.profiles") is not None
    assert importlib.util.find_spec("src.app.helpers") is not None
    assert importlib.util.find_spec("src.app.window") is not None
    assert importlib.util.find_spec("src.ui.dialogs.video") is not None
    assert importlib.util.find_spec("src.ui.dialogs.camera") is not None
    assert importlib.util.find_spec("src.ui.dialogs.kaderblick") is not None
    assert importlib.util.find_spec("src.ui.dialogs.shutdown") is not None
    assert importlib.util.find_spec("src.ui.job_editor.dialog") is not None
    assert importlib.util.find_spec("src.ui.job_editor.pages") is not None
    assert importlib.util.find_spec("src.runtime.workflow_executor.core") is not None
    assert importlib.util.find_spec("src.runtime.workflow_executor.pipeline") is not None

    assert importlib.util.find_spec("src.media.converter") is not None
    assert importlib.util.find_spec("src.media.ffmpeg_runner") is not None
    assert importlib.util.find_spec("src.transfer.downloader") is not None
    assert importlib.util.find_spec("src.integrations.kaderblick") is not None
    assert importlib.util.find_spec("src.integrations.youtube_title_editor") is not None
    assert importlib.util.find_spec("src.runtime.workflow_executor") is not None
    assert importlib.util.find_spec("src.ui.dialogs") is not None
    assert importlib.util.find_spec("src.ui.job_editor") is not None

    assert importlib.util.find_spec("src.converter") is None
    assert importlib.util.find_spec("src.ffmpeg_runner") is None
    assert importlib.util.find_spec("src.downloader") is None
    assert importlib.util.find_spec("src.kaderblick") is None
    assert importlib.util.find_spec("src.youtube") is None
    assert importlib.util.find_spec("src.youtube_title_editor") is None
    assert importlib.util.find_spec("src.worker") is None
    assert importlib.util.find_spec("src.workflow_executor") is None
    assert importlib.util.find_spec("src.dialogs") is None
    assert importlib.util.find_spec("src.job_editor") is None