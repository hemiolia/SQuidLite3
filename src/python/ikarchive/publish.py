"""同期中もGUIを更新する。旧分析.xlsxは生成・更新しない。"""
from .gui import write_gui
from .store import now

def publish_outputs(store):
    root=store.output_root/'exports'
    try:
        page=write_gui(store,root/'gui'/'index.html')
    except Exception as exc:
        # A GUI generation failure never erases collected records or masquerades as success.
        with store.db:
            store._control('export_error',type(exc).__name__)
            store.issue('EXPORT_FAILED',{'error_type':type(exc).__name__})
        return {'error':type(exc).__name__}
    with store.db:
        store._control('exports_updated_at',now())
        store.db.execute("DELETE FROM control WHERE key='export_error'")
    return {'scope':'gui_only','gui':page}
