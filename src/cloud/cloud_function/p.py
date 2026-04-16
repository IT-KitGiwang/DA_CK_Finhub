@job(hooks={email_on_success, email_on_failure})
def finhub_realtime_setup_pipeline():
    """Đường ống Khởi tạo Data Warehouse chuẩn Lambda Architecture"""
    # Giai đoạn Tiên quyết rễ
    step1 = boot_infrastructure()
    step2 = cleanup_old_garbage(step1)
    step3 = activate_thrift_server(step2)
    
    # Giai đoạn Phân nhánh (Chạy song song)
    step4a = load_dimension_tables(step3)
    step4b = config_superset(step3)
    
    # Giai đoạn Chuyển đổi (Triển khai Công nhân Streaming)pj
    step5 = start_realtime_streams(step4a, step4b)
    
    # Giai đoạn Trực quan (View Abstraction)
    create_star_views(step5)
