import { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Search,
  Database,
  FileSpreadsheet,
  Cloud,
  BarChart3,
  CheckCircle2,
  Loader,
  X,
  ChevronRight,
  Plug,
  AlertCircle,
  Server,
  Globe,
  Info,
  ArrowRight,
  ShieldCheck,
  FileCode,
  Activity
} from 'lucide-react';
import {
  getConnectorCatalog,
  validateConnectorIngest,
  fetchConnectorIngest,
} from '../services/api';

const CATEGORIES = [
  { id: 'all', label: 'All', icon: Plug },
  { id: 'file', label: 'File', icon: FileSpreadsheet },
  { id: 'database', label: 'Database', icon: Database },
  { id: 'saas', label: 'Online Services', icon: Cloud },
  { id: 'analytics', label: 'Analytics', icon: BarChart3 },
];

const UploadPage = () => {
  const navigate = useNavigate();

  // Catalog State
  const [catalog, setCatalog] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('all');

  // Selection & Modal State
  const [selected, setSelected] = useState(null);
  const [step, setStep] = useState(1); // 1: Credentials, 2: Select Source, 3: Importing
  const [formValues, setFormValues] = useState({});
  const [uploadFile, setUploadFile] = useState(null);
  
  // Validation/Import State
  const [validating, setValidating] = useState(false);
  const [importing, setImporting] = useState(false);
  const [modalError, setModalError] = useState('');
  const [sources, setSources] = useState([]);
  const [selectedSource, setSelectedSource] = useState('');
  const [logs, setLogs] = useState([]);
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    fetchCatalog();
  }, []);

  const fetchCatalog = async () => {
    try {
      setLoading(true);
      const res = await getConnectorCatalog();
      if (res.success) {
        setCatalog(res.data || []);
      } else {
        setError(res.message || 'Failed to load connectors');
      }
    } catch (e) {
      setError(e.message || 'Server error');
    } finally {
      setLoading(false);
    }
  };

  const addLog = (msg, type = 'info') => {
    setLogs(prev => [...prev, { id: Date.now(), msg, type, time: new Date().toLocaleTimeString() }]);
  };

  const filteredCatalog = useMemo(() => {
    return catalog.filter(c => {
      const matchSearch = c.label.toLowerCase().includes(search.toLowerCase()) || 
                          c.description.toLowerCase().includes(search.toLowerCase());
      const matchCat = selectedCategory === 'all' || c.category === selectedCategory;
      return matchSearch && matchCat;
    });
  }, [catalog, search, selectedCategory]);

  const handleConnectorClick = (c) => {
    setSelected(c);
    setStep(1);
    setFormValues({});
    setUploadFile(null);
    setModalError('');
    setSources([]);
    setSelectedSource('');
    setLogs([{ id: 1, msg: `Initiating ${c.label} connector...`, type: 'info', time: new Date().toLocaleTimeString() }]);
    setProgress(0);
  };

  const closeModal = () => {
    if (importing) return;
    setSelected(null);
  };

  const handleInputChange = (name, value) => {
    setFormValues(prev => ({ ...prev, [name]: value }));
  };

  const handleFileChange = (e) => {
    const file = e.target.files[0];
    if (file) {
      setUploadFile(file);
      addLog(`Selected file: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`, 'success');
    }
  };

  const validateCredentials = async () => {
    setValidating(true);
    setModalError('');
    addLog('Validating credentials and connecting to source...', 'info');
    setProgress(30);

    try {
      const formData = new FormData();
      formData.append('connector', selected.id);
      formData.append('credentials', JSON.stringify(formValues));
      if (uploadFile) formData.append('file', uploadFile);

      const res = await validateConnectorIngest(formData);
      if (res.success && res.data.status === 'ok') {
        setSources(res.data.sources || []);
        setStep(2);
        addLog('Connection established successfully!', 'success');
        setProgress(60);
      } else {
        const msg = res.message || res.data?.message || 'Validation failed';
        setModalError(msg);
        addLog(`Validation Error: ${msg}`, 'error');
        setProgress(0);
      }
    } catch (e) {
      setModalError(e.message);
      addLog(`System Error: ${e.message}`, 'error');
    } finally {
      setValidating(false);
    }
  };

  const handleImport = async () => {
    setImporting(true);
    setStep(3);
    addLog(`Starting import from source: ${selectedSource}...`, 'info');
    setProgress(80);

    try {
      const formData = new FormData();
      formData.append('connector', selected.id);
      formData.append('source', selectedSource);
      formData.append('credentials', JSON.stringify(formValues));
      if (uploadFile) formData.append('file', uploadFile);

      const res = await fetchConnectorIngest(formData);
      if (res.success) {
        addLog('Import complete! Dataset registered in PostgreSQL.', 'success');
        setProgress(100);
        setTimeout(() => {
          navigate(res.redirect || '/admin/datasets');
        }, 1500);
      } else {
        setModalError(res.message || 'Import failed');
        addLog(`Import Error: ${res.message}`, 'error');
        setStep(2);
      }
    } catch (e) {
      setModalError(e.message);
      addLog(`System Error: ${e.message}`, 'error');
      setStep(2);
    } finally {
      setImporting(false);
    }
  };

  if (loading) return (
    <div className="flex-center" style={{ height: '80vh', flexDirection: 'column', gap: '1rem' }}>
      <Loader className="spinner" size={40} color="var(--primary)" />
      <span style={{ color: 'var(--text-muted)', fontFamily: 'DM Mono' }}>LOADING CATALOG...</span>
    </div>
  );

  return (
    <div className="container view-enter" style={{ maxWidth: '1400px' }}>
      <header style={{ marginBottom: '2rem', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end' }}>
        <div>
          <h1 className="gradient-text" style={{ fontSize: '2.5rem', marginBottom: '0.5rem' }}>Get Data</h1>
          <p style={{ color: 'var(--text-muted)', fontSize: '1.1rem' }}>
            Choose a data source to begin your intelligence journey.
          </p>
        </div>
        <div className="admin-search-bar" style={{ minWidth: '350px', height: '48px' }}>
          <Search size={18} />
          <input 
            placeholder="Search connectors (e.g. SQL, Sheets, CSV)..." 
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '260px 1fr', gap: '2rem', height: 'calc(100vh - 250px)' }}>
        {/* Categories Sidebar */}
        <div className="glass-panel" style={{ padding: '1rem' }}>
          <h3 style={{ fontSize: '0.9rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1.5px', marginBottom: '1.5rem', paddingLeft: '0.5rem' }}>
            Categories
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
            {CATEGORIES.map(cat => (
              <button
                key={cat.id}
                onClick={() => setSelectedCategory(cat.id)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '12px',
                  padding: '12px 16px',
                  borderRadius: '10px',
                  border: 'none',
                  background: selectedCategory === cat.id ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
                  color: selectedCategory === cat.id ? 'var(--primary)' : 'var(--text-muted)',
                  cursor: 'pointer',
                  textAlign: 'left',
                  transition: 'all 0.2s',
                  fontWeight: selectedCategory === cat.id ? '600' : '400',
                  fontSize: '0.95rem'
                }}
              >
                <cat.icon size={18} />
                {cat.label}
              </button>
            ))}
          </div>
          <div style={{ marginTop: 'auto', padding: '1rem', borderTop: '1px solid var(--border-color)', position: 'absolute', bottom: 0, width: 'calc(100% - 2rem)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--success)', fontSize: '0.8rem' }}>
              <ShieldCheck size={14} />
              <span>Admin Authorized</span>
            </div>
          </div>
        </div>

        {/* Connector Grid */}
        <div style={{ overflowY: 'auto', paddingRight: '0.5rem' }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: '1.2rem' }}>
            {filteredCatalog.map(c => (
              <div
                key={c.id}
                className="glass-panel"
                onClick={() => handleConnectorClick(c)}
                style={{
                  padding: '1.5rem',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  position: 'relative',
                  overflow: 'hidden',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '1rem',
                  border: selected?.id === c.id ? '1px solid var(--primary)' : '1px solid var(--border-color)'
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.transform = 'translateY(-4px)';
                  e.currentTarget.style.borderColor = 'rgba(88, 166, 255, 0.4)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.transform = 'translateY(0)';
                  e.currentTarget.style.borderColor = selected?.id === c.id ? 'var(--primary)' : 'var(--border-color)';
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div style={{ 
                    width: '48px', 
                    height: '48px', 
                    borderRadius: '12px', 
                    background: 'rgba(255,255,255,0.05)', 
                    display: 'flex', 
                    alignItems: 'center', 
                    justifyContent: 'center',
                    color: 'var(--primary)'
                  }}>
                    {c.category === 'file' && <FileSpreadsheet size={24} />}
                    {c.category === 'database' && <Server size={24} />}
                    {c.category === 'saas' && <Globe size={24} />}
                    {c.category === 'analytics' && <BarChart3 size={24} />}
                  </div>
                  <span style={{ 
                    fontSize: '0.7rem', 
                    fontFamily: 'DM Mono', 
                    padding: '3px 8px', 
                    background: 'rgba(255,255,255,0.05)', 
                    borderRadius: '6px',
                    color: 'var(--text-muted)',
                    textTransform: 'uppercase'
                  }}>
                    {c.tier}
                  </span>
                </div>
                <div>
                  <h4 style={{ fontSize: '1.1rem', marginBottom: '0.4rem' }}>{c.label}</h4>
                  <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem', lineHeight: '1.4' }}>
                    {c.description}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Ingestion Modal (Dialog) */}
      {selected && (
        <div style={{
          position: 'fixed',
          inset: 0,
          background: 'rgba(0,0,0,0.85)',
          backdropFilter: 'blur(8px)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 1000,
          padding: '2rem'
        }}>
          <div className="glass-panel" style={{ 
            width: '100%', 
            maxWidth: '1000px', 
            height: '700px', 
            display: 'grid', 
            gridTemplateColumns: '350px 1fr',
            overflow: 'hidden'
          }}>
            {/* Modal Sidebar (Info & Logs) */}
            <div className="modal-sidebar-info" style={{ 
              background: 'rgba(0,0,0,0.2)', 
              borderRight: '1px solid var(--border-color)',
              display: 'flex',
              flexDirection: 'column',
              overflowY: 'auto'
            }}>
              <div style={{ padding: '2rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '1.5rem' }}>
                  <div style={{ width: '40px', height: '40px', borderRadius: '10px', background: 'var(--primary-glow)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--primary)' }}>
                    <Plug size={20} />
                  </div>
                  <h3 style={{ fontSize: '1.2rem' }}>{selected.label}</h3>
                </div>
                
                {/* Dynamic Setup Guide */}
                {selected.setup_guide && (
                  <div style={{ background: 'rgba(255,255,255,0.03)', borderRadius: '12px', padding: '1.2rem', marginBottom: '1.5rem', border: '1px solid rgba(255,255,255,0.05)' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '1rem' }}>
                      <Info size={16} style={{ color: 'var(--primary)' }} />
                      <span style={{ fontSize: '0.85rem', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Setup Guide</span>
                    </div>
                    <ul style={{ padding: 0, margin: 0, listStyle: 'none' }}>
                      {selected.setup_guide.steps.map((step, i) => (
                        <li key={i} style={{ display: 'flex', gap: '10px', marginBottom: '12px', fontSize: '0.8rem', lineHeight: '1.4', color: 'var(--text-muted)' }}>
                          <span style={{ width: '18px', height: '18px', borderRadius: '50%', background: 'rgba(255,255,255,0.1)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '10px', fontWeight: '700', flexShrink: 0 }}>{i + 1}</span>
                          {step}
                        </li>
                      ))}
                    </ul>
                    {selected.setup_guide.docs && (
                      <a href={selected.setup_guide.docs} target="_blank" rel="noreferrer" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px', marginTop: '1rem', padding: '8px', background: 'rgba(255,255,255,0.05)', borderRadius: '8px', color: 'var(--primary)', fontSize: '0.75rem', textDecoration: 'none', fontWeight: '600' }}>
                        View Documentation <ChevronRight size={14} />
                      </a>
                    )}
                  </div>
                )}

                {/* Operational Logs */}
                <div style={{ background: 'rgba(0,0,0,0.3)', borderRadius: '10px', padding: '1rem', height: '250px', overflowY: 'auto', fontFamily: 'DM Mono', fontSize: '0.75rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '0.8rem', color: 'var(--primary)', fontWeight: '600' }}>
                    <Activity size={14} /> <span>LIVE LOGS</span>
                  </div>
                  {logs.map(log => (
                    <div key={log.id} style={{ marginBottom: '4px', opacity: 0.8 }}>
                      <span style={{ color: 'var(--text-muted)', marginRight: '6px' }}>[{log.time}]</span>
                      <span style={{ color: log.type === 'error' ? '#ff4d4d' : log.type === 'success' ? '#00e676' : '#fff' }}>{log.msg}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Modal Main Content (Steps) */}
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <div style={{ padding: '1.5rem 2rem', borderBottom: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', gap: '2rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: step >= 1 ? 'var(--primary)' : 'var(--text-muted)', fontWeight: step === 1 ? '600' : '400' }}>
                    <span style={{ width: '20px', height: '20px', borderRadius: '50%', border: '1px solid currentColor', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.7rem' }}>1</span>
                    <span style={{ fontSize: '0.85rem' }}>Connect</span>
                  </div>
                  <ChevronRight size={16} color="var(--border-color)" />
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: step >= 2 ? 'var(--primary)' : 'var(--text-muted)', fontWeight: step === 2 ? '600' : '400' }}>
                    <span style={{ width: '20px', height: '20px', borderRadius: '50%', border: '1px solid currentColor', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.7rem' }}>2</span>
                    <span style={{ fontSize: '0.85rem' }}>Transform</span>
                  </div>
                  <ChevronRight size={16} color="var(--border-color)" />
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: step >= 3 ? 'var(--primary)' : 'var(--text-muted)', fontWeight: step === 3 ? '600' : '400' }}>
                    <span style={{ width: '20px', height: '20px', borderRadius: '50%', border: '1px solid currentColor', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.7rem' }}>3</span>
                    <span style={{ fontSize: '0.85rem' }}>Import</span>
                  </div>
                </div>
                <button 
                  onClick={closeModal} 
                  style={{ background: 'transparent', border: 'none', color: 'var(--text-muted)', cursor: 'pointer' }}
                >
                  <X size={20} />
                </button>
              </div>

              <div style={{ flex: 1, padding: '2.5rem', overflowY: 'auto' }}>
                {modalError && (
                  <div style={{ 
                    background: 'rgba(248, 81, 73, 0.1)', 
                    border: '1px solid var(--danger)', 
                    borderRadius: '8px', 
                    padding: '1rem', 
                    color: 'var(--danger)', 
                    display: 'flex', 
                    gap: '12px',
                    marginBottom: '2rem'
                  }}>
                    <AlertCircle size={20} />
                    <span style={{ fontSize: '0.9rem' }}>{modalError}</span>
                  </div>
                )}

                {step === 1 && (
                  <div className="view-enter">
                    <h2 style={{ fontSize: '1.5rem', marginBottom: '1.5rem' }}>Configure Connection</h2>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
                      {selected.config_schema?.fields?.map(field => (
                        <div key={field.name} style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
                          <label style={{ fontSize: '0.85rem', fontWeight: '600', color: '#e0e0e0', display: 'flex', justifyContent: 'space-between' }}>
                            <span>{field.label} {field.required && <span style={{ color: 'var(--primary)' }}>*</span>}</span>
                          </label>
                          
                          {field.type === 'file' || field.type === 'service_account_file' ? (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                              <div style={{ 
                                border: '2px dashed var(--border-color)', 
                                borderRadius: '12px', 
                                padding: '1.5rem', 
                                textAlign: 'center',
                                cursor: 'pointer',
                                background: uploadFile ? 'rgba(var(--primary-rgb), 0.05)' : 'transparent',
                                borderColor: uploadFile ? 'var(--primary)' : 'var(--border-color)',
                                transition: 'all 0.2s'
                              }}
                              onClick={() => document.getElementById(`file-${field.name}`).click()}
                              >
                                <input 
                                  id={`file-${field.name}`} 
                                  type="file" 
                                  hidden 
                                  onChange={handleFileChange}
                                />
                                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.5rem' }}>
                                  {uploadFile ? <CheckCircle2 color="var(--primary)" size={24} /> : <FileCode color="var(--text-muted)" size={24} />}
                                  <span style={{ fontSize: '0.85rem', color: uploadFile ? '#fff' : 'var(--text-muted)' }}>
                                    {uploadFile ? uploadFile.name : (field.placeholder || 'Select or drop file here')}
                                  </span>
                                </div>
                              </div>
                              {field.hint && <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontStyle: 'italic' }}>{field.hint}</span>}
                            </div>
                          ) : (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                              <input
                                type={field.type}
                                placeholder={field.placeholder || ''}
                                value={formValues[field.name] || ''}
                                onChange={(e) => handleInputChange(field.name, e.target.value)}
                                style={{
                                  background: 'rgba(255,255,255,0.05)',
                                  border: '1px solid var(--border-color)',
                                  borderRadius: '8px',
                                  padding: '12px 16px',
                                  color: '#fff',
                                  outline: 'none',
                                  fontSize: '0.95rem',
                                  transition: 'border-color 0.2s'
                                }}
                                onFocus={(e) => e.target.style.borderColor = 'var(--primary)'}
                                onBlur={(e) => e.target.style.borderColor = 'var(--border-color)'}
                              />
                              {field.hint && <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>{field.hint}</span>}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {step === 2 && (
                  <div className="view-enter">
                    <h2 style={{ fontSize: '1.5rem', marginBottom: '0.5rem' }}>Select Data Source</h2>
                    <p style={{ color: 'var(--text-muted)', marginBottom: '2rem' }}>We discovered the following sources from your connection. Select one to import.</p>
                    
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '1rem' }}>
                      {sources.map(source => (
                        <div
                          key={source.id}
                          onClick={() => setSelectedSource(source.id)}
                          style={{
                            padding: '1.2rem',
                            background: selectedSource === source.id ? 'rgba(88, 166, 255, 0.1)' : 'rgba(255,255,255,0.03)',
                            border: `1px solid ${selectedSource === source.id ? 'var(--primary)' : 'var(--border-color)'}`,
                            borderRadius: '12px',
                            cursor: 'pointer',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: '0.5rem',
                            transition: 'all 0.2s'
                          }}
                        >
                          <Database size={20} color={selectedSource === source.id ? 'var(--primary)' : 'var(--text-muted)'} />
                          <span style={{ fontWeight: '600', fontSize: '0.95rem' }}>{source.label}</span>
                          {source.meta?.preview_rows && (
                            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>{source.meta.preview_rows} rows found</span>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {step === 3 && (
                  <div className="view-enter flex-center" style={{ height: '100%', flexDirection: 'column', textAlign: 'center' }}>
                    <div style={{ position: 'relative', width: '120px', height: '120px', marginBottom: '2rem' }}>
                      <div className="spinner" style={{ 
                        position: 'absolute', 
                        inset: 0, 
                        border: '4px solid rgba(88, 166, 255, 0.1)', 
                        borderTopColor: 'var(--primary)', 
                        borderRadius: '50%' 
                      }}></div>
                      <div style={{ 
                        position: 'absolute', 
                        inset: 0, 
                        display: 'flex', 
                        alignItems: 'center', 
                        justifyContent: 'center',
                        fontSize: '1.5rem',
                        fontWeight: '700',
                        fontFamily: 'DM Mono'
                      }}>
                        {progress}%
                      </div>
                    </div>
                    <h2 style={{ fontSize: '1.8rem', marginBottom: '0.5rem' }}>Importing Intelligence</h2>
                    <p style={{ color: 'var(--text-muted)', maxWidth: '400px' }}>
                      Please wait while we transfer and normalize your data in our secure environment.
                    </p>
                  </div>
                )}
              </div>

              {/* Modal Actions */}
              <div style={{ padding: '1.5rem 2.5rem', borderTop: '1px solid var(--border-color)', display: 'flex', justifyContent: 'flex-end', gap: '1rem' }}>
                <button 
                  onClick={closeModal}
                  disabled={importing}
                  style={{ background: 'transparent', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: '0.9rem' }}
                >
                  Cancel
                </button>
                
                {step === 1 && (
                  <button 
                    className="btn-primary" 
                    onClick={validateCredentials} 
                    disabled={validating}
                    style={{ minWidth: '160px' }}
                  >
                    {validating ? <Loader size={18} className="spinner" /> : 'Connect'}
                    {!validating && <ArrowRight size={18} />}
                  </button>
                )}

                {step === 2 && (
                  <button 
                    className="btn-primary" 
                    onClick={handleImport}
                    disabled={!selectedSource || importing}
                    style={{ minWidth: '160px' }}
                  >
                    {importing ? <Loader size={18} className="spinner" /> : 'Import Data'}
                    {!importing && <CheckCircle2 size={18} />}
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
      <style>{`
        @media (max-width: 1024px) {
          .container { padding: 1rem; }
          .admin-search-bar { min-width: 250px; }
        }
        @media (max-width: 768px) {
          header { flex-direction: column; align-items: flex-start !important; gap: 1rem; }
          .admin-search-bar { width: 100%; min-width: 100%; }
          .container > div { grid-template-columns: 1fr !important; }
          .glass-panel[style*="260px"] { display: none; } /* Hide sidebar on mobile or use a dropdown */
        }
        
        .modal-content-grid {
          display: grid;
          grid-template-columns: 350px 1fr;
          width: 100%;
          max-width: 1000px;
          height: 700px;
        }
        
        @media (max-width: 900px) {
          .modal-content-grid {
            grid-template-columns: 1fr;
            height: auto;
            max-height: 90vh;
            overflow-y: auto;
          }
          .modal-sidebar-info { display: none; }
        }

        .connector-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
          gap: 1.2rem;
        }
        @media (max-width: 600px) {
          .connector-grid {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </div>
  );
};

export default UploadPage;
