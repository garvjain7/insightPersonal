import { useState, useEffect, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import './EmployeeCleaningPage.css';
import { getDatasetPreview, finalizeDataset, pauseCleaning, initCleaningWorkspace, getCleaningState, previewCleaningStep, applyCleaningStep, downloadDataset } from '../../services/api';


const STEPS = [
  { id: 1, name: 'Null Values', shortName: 'Null Values', statusKey: 'null_values' },
  { id: 2, name: 'Duplicates', shortName: 'Duplicates', statusKey: 'duplicates' },
  { id: 3, name: 'Data Types', shortName: 'Data Types', statusKey: 'data_types' },
  { id: 4, name: 'Outliers', shortName: 'Outliers', statusKey: 'outliers' },
  { id: 5, name: 'Feature Eng.', shortName: 'Feature Eng.', statusKey: 'feature_engineering' },
];

const NULL_STRATEGIES = ['Keep as-is', 'Fill with 0', 'Fill with mean', 'Fill with median', 'Fill with mode', 'Drop rows'];
const DUPE_STRATEGIES = [
  { id: 'Keep first', label: 'Keep First Occurrence', desc: 'Remove all but first duplicate row' },
  { id: 'Keep last', label: 'Keep Last Occurrence', desc: 'Remove all but last duplicate row' },
  { id: 'Keep as-is', label: 'Ignore', desc: 'Keep all rows as-is' }
];

const TYPE_STRATEGIES = ['Auto-detect', 'String', 'Integer', 'Float', 'Date', 'Boolean'];
const OUTLIER_STRATEGIES = ['Keep as-is', 'Remove rows', 'IQR capping', 'Z-score capping'];

const EmployeeCleaningPage = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const dsId = searchParams.get('ds');
  const dsName = searchParams.get('name') || 'Dataset';

  const [currentStep, setCurrentStep] = useState(1);
  const [verifyOpen, setVerifyOpen] = useState(false);
  const [showFullCleaned, setShowFullCleaned] = useState(false);
  const [loading, setLoading] = useState(true);
  
  const [tableRows, setTableRows] = useState([]);
  const [tableHeaders, setTableHeaders] = useState([]);
  
  const [settings, setSettings] = useState({
    1: {}, 2: { strategy: 'Keep first' }, 3: {}, 4: {}, 5: {}
  });

  const [cleaningState, setCleaningState] = useState(null);

  const [leftWidth, setLeftWidth] = useState(68);
  const [dragging, setDragging] = useState(false);

  const [featStreaming, setFeatStreaming] = useState(false);
  const [featDone, setFeatDone] = useState(false);
  const [featStreamText, setFeatStreamText] = useState('');
  const STREAM_LINES = [
    '> Connecting to Ollama (llama3.2)...',
    '> Analyzing schema...',
    '> Detecting column relationships...',
    '> Computing value distributions...',
    '> Generating feature suggestions...'
  ];
  const [aiSuggestions, setAiSuggestions] = useState([]);
  const [featStatuses, setFeatStatuses] = useState({});
  const [error, setError] = useState(null);
  const [page, setPage] = useState(1);
  const [totalRows, setTotalRows] = useState(0);
  const [colFilter, setColFilter] = useState('all');
  const [rawStats, setRawStats] = useState({
    totalRows: 0,
    totalNulls: 0,
    totalDuplicates: 0,
    totalOutliers: 0,
    columnNulls: {},
    columns: [],
    numericColumns: [],
  });

  // Ref to suppress the live-preview useEffect when an explicit action
  // (Skip / Let AI Decide) is already running its own preview.
  const suppressLivePreview = useRef(false);


  const fetchData = async () => {
    if (!dsId) {
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      // 1. Get current cleaning state
      const stateRes = await getCleaningState(dsId);
      if (stateRes.success) {
        setCleaningState(stateRes.state || null);
        
        if (stateRes.state?.current_step) {
          const sIdx = STEPS.findIndex(s => s.statusKey === stateRes.state.current_step);
          if (sIdx !== -1) setCurrentStep(sIdx + 1);
        }

        if (stateRes.metadata) {
          const newSettings = { ...settings };
          STEPS.forEach((s, i) => {
            if (stateRes.metadata[s.statusKey]?.params) {
              newSettings[i + 1] = stateRes.metadata[s.statusKey].params;
            }
          });
          setSettings(newSettings);
        }

        if (stateRes.state?.stats) setRawStats(stateRes.state.stats);
      } else if (!stateRes.initialised) {
        await initCleaningWorkspace(dsId);
        const newState = await getCleaningState(dsId);
        if (newState.success) {
          setCleaningState(newState.state);
          if (newState.state?.stats) setRawStats(newState.state.stats);
        }
      }

      // 2. Load preview (Source 'preview' if active_preview matches current step, else 'working')
      const source = (stateRes.state?.active_preview === STEPS[currentStep - 1]?.statusKey) ? 'preview' : 'working';
      const data = await getDatasetPreview(dsId, page, source);

      if (data.success) {
        setTableRows(data.data || []);
        setTotalRows(data.totalRows || 0);
        if (data.currentStats) {
          setRawStats(prev => ({
            ...prev,
            ...data.currentStats,
            // Compute numeric columns from column_nulls keys — backend always returns this
            numericColumns: (data.currentStats.numericColumns || prev.numericColumns),
          }));
        }
        if (data.data?.length > 0) setTableHeaders(Object.keys(data.data[0]));
      } else {
        setError(data.message || "Failed to load dataset preview.");
      }
    } catch (err) {
      setError(err.message || "Failed to load data.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [dsId, page]);

  const handlePreview = async (stepId, config, isAi = false, mode = 'preview') => {
    if (!dsId) return;
    setLoading(true);
    try {
      const res = await previewCleaningStep(dsId, stepId, config, isAi, mode);
      if (res.success) {
        // Refresh preview table data from 'preview' source
        const previewData = await getDatasetPreview(dsId, 1, 'preview');
        if (previewData.success) {
          setTableRows(previewData.data || []);
          setPage(1);
          if (previewData.currentStats) setRawStats(previewData.currentStats);
        }
        
        // Refresh cleaning state to get new 'previewed' status
        const stateRes = await getCleaningState(dsId);
        if (stateRes.success) {
          setCleaningState(stateRes.state);

        }

        if (isAi && res.decisions) {
          setSettings(prev => ({
            ...prev,
            [stepId]: { ...prev[stepId], ...res.decisions }
          }));
        }
      } else {
        setError(res.message || "Preview failed");
      }
    } catch (err) {
      setError(err.message || "Preview error");
    } finally {
      setLoading(false);
    }
  };

  // Track page exit (Pause Logging)
  useEffect(() => {
    return () => {
      if (dsId) {
        pauseCleaning(dsId).catch(() => {});
      }
    };
  }, [dsId]);

  // Handle Drag Resizing

  useEffect(() => {
    const handleMouseMove = (e) => {
      if (!dragging) return;
      const pct = Math.min(85, Math.max(25, (e.clientX / window.innerWidth) * 100));
      setLeftWidth(pct);
    };
    const handleMouseUp = () => setDragging(false);
    if (dragging) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [dragging]);



  const handleApplyFinal = async () => {
    if (!dsId) return;
    setLoading(true);
    try {
      const res = await applyCleaningStep(dsId, currentStep);
      if (res.success) {
        if (currentStep < 5) {
          // Increment step
          const nextS = currentStep + 1;
          setCurrentStep(nextS);
          if (nextS === 5 && !featDone && !featStreaming) startFeatStream();
        }
        // On step 5: caller opens modal — don't auto-open here

        // Refresh data from 'working' source
        const workingData = await getDatasetPreview(dsId, 1, 'working');
        if (workingData.success) {
          setTableRows(workingData.data || []);
          setPage(1);
          if (workingData.currentStats) setRawStats(workingData.currentStats);
        }

        // Refresh cleaning state
        const stateRes = await getCleaningState(dsId);
        if (stateRes.success) {
          setCleaningState(stateRes.state);

        }
      } else {
        setError(res.message || "Apply failed");
      }
    } catch (err) {
      setError(err.message || "Apply error");
    } finally {
      setLoading(false);
    }
  };

  const handleSkip = async () => {
    suppressLivePreview.current = true;
    await handlePreview(currentStep, {}, false, 'skip');
    suppressLivePreview.current = false;
  };

  const handleLetAiDecide = async () => {
    suppressLivePreview.current = true;
    await handlePreview(currentStep, { ai: true }, true, 'preview');
    suppressLivePreview.current = false;
  };

  // Live preview for manual dropdown changes only
  useEffect(() => {
    if (suppressLivePreview.current) return;
    const stepKey = STEPS[currentStep - 1]?.statusKey;
    if (cleaningState?.steps?.[stepKey] === 'committed') return;
    const config = { params: settings[currentStep] };
    const timer = setTimeout(() => {
      handlePreview(currentStep, config, false, 'preview');
    }, 800);
    return () => clearTimeout(timer);
  }, [settings[currentStep]]);

  // Build feature suggestions from column names + numeric columns
  const buildFeatureSuggestions = (headers, numericCols) => {
    const slug = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
    const numericSet = new Set(numericCols || []);
    const normalize = (s) => s.toLowerCase();
    const isNumericHeader = (h) => numericSet.has(h);
    const matchesToken = (header, tokens) => {
      const key = normalize(header);
      return tokens.some(token => new RegExp(`(^|[^a-z0-9])${token}([^a-z0-9]|$)`, 'i').test(key));
    };
    const isLikelyCategorical = (header) => /channel|rep|region|type|method|status|name|id|code/.test(normalize(header));
    const find = (tokens, { numericOnly = false, excludeCategorical = false } = {}) => {
      const candidates = headers.filter((h) => {
        if (!matchesToken(h, tokens)) return false;
        if (numericOnly && !isNumericHeader(h)) return false;
        if (excludeCategorical && isLikelyCategorical(h)) return false;
        return true;
      });
      if (candidates.length > 0) return candidates[0];
      if (!numericOnly) {
        const fallback = headers.find((h) => matchesToken(h, tokens));
        if (fallback) return fallback;
      }
      return null;
    };

    const suggestions = [];
    const seen = new Set();
    const add = (s) => { if (!seen.has(s.col)) { seen.add(s.col); suggestions.push({ id: suggestions.length, ...s }); } };

    const salesCol    = find(['sales_amount', 'sales', 'revenue', 'amount', 'total'], { numericOnly: true, excludeCategorical: true });
    const quantityCol = find(['quantity', 'qty', 'units', 'count'], { numericOnly: true });
    const profitCol   = find(['profit', 'margin', 'gain'], { numericOnly: true });
    const discountCol = find(['discount', 'reduction'], { numericOnly: true });
    const priceCol    = find(['unit_price', 'price', 'unit_cost', 'cost', 'rate', 'fee'], { numericOnly: true });
    const lengthCol   = find(['length', 'len'], { numericOnly: true });
    const widthCol    = find(['width', 'breadth', 'wid'], { numericOnly: true });
    const heightCol   = find(['height', 'depth', 'ht'], { numericOnly: true });

    if (salesCol && quantityCol) add({ col: `sales_per_${slug(quantityCol)}`, type: 'ratio', operation: 'ratio', inputs: [salesCol, quantityCol], formula: `${salesCol} / ${quantityCol}`, desc: 'Per-unit sales value.' });
    if (profitCol && salesCol)   add({ col: 'profit_margin', type: 'ratio', operation: 'ratio', inputs: [profitCol, salesCol], formula: `${profitCol} / ${salesCol}`, desc: 'Profit margin percentage.' });
    if (discountCol && priceCol) add({ col: 'discount_rate', type: 'ratio', operation: 'ratio', inputs: [discountCol, priceCol], formula: `${discountCol} / ${priceCol}`, desc: 'Discount as fraction of price.' });
    if (lengthCol && widthCol)   add({ col: `area_${slug(lengthCol)}_${slug(widthCol)}`, type: 'product', operation: 'product', inputs: [lengthCol, widthCol], formula: `${lengthCol} × ${widthCol}`, desc: 'Area from two dimension columns.' });
    if (lengthCol && widthCol && heightCol) add({ col: `volume_${slug(lengthCol)}`, type: 'product', operation: 'product', inputs: [lengthCol, widthCol, heightCol], formula: `${lengthCol} × ${widthCol} × ${heightCol}`, desc: 'Volume from three dimension columns.' });

    // Log transforms for skewed numeric cols
    numericCols.slice(0, 3).forEach(col => {
      if (!salesCol && !profitCol) return; // only add log if no ratio features yet
      add({ col: `${slug(col)}_log`, type: 'log', operation: 'log', inputs: [col], formula: `log1p(${col})`, desc: `Log transform of ${col} to reduce skew.` });
    });

    // Fallback: normalize first numeric col
    if (suggestions.length === 0 && numericCols.length > 0) {
      const col = numericCols[0];
      add({ col: `${slug(col)}_normalized`, type: 'normalize', operation: 'normalize', inputs: [col], formula: `${col} / max(${col})`, desc: `Scales ${col} to 0–1 range.` });
    }

    return suggestions;
  };

  const startFeatStream = () => {
    setFeatStreaming(true);
    setFeatDone(false);
    setFeatStreamText('');
    let line = 0, ch = 0, text = '';

    // Build real suggestions from column knowledge
    const outlierCols = getNumCols();
    const suggestions = buildFeatureSuggestions(tableHeaders, outlierCols);
    setFeatStatuses({});
    setAiSuggestions(suggestions);
    
    const tick = () => {
      if (line >= STREAM_LINES.length) {
        setTimeout(() => {
          setFeatStreaming(false);
          setFeatDone(true);
        }, 500);
        return;
      }
      const L = STREAM_LINES[line];
      if (ch < L.length) {
        text += L[ch++];
        setFeatStreamText(text);
        setTimeout(tick, 20);
      } else {
        text += '\n'; line++; ch = 0;
        setFeatStreamText(text);
        setTimeout(tick, 200);
      }
    };
    tick();
  };

  // Columns that are useless for outlier detection
  const ID_DATE_PATTERN = /\b(id|_id|date|time|timestamp|created|updated|at|year|month|day)\b/i;

  const getNumCols = () => {
    const isDateCol = (col) => {
      // Heuristic: if >50% of first-page values parse as dates
      const sample = tableRows.slice(0, 20).map(r => r[col]).filter(Boolean);
      if (sample.length === 0) return false;
      const parseable = sample.filter(v => !isNaN(Date.parse(v)) && isNaN(Number(v)));
      return parseable.length / sample.length > 0.5;
    };

    const isIdCol = (col) => ID_DATE_PATTERN.test(col);

    // Prefer backend-reported numeric columns
    const candidates = (rawStats.numericColumns && rawStats.numericColumns.length > 0)
      ? tableHeaders.filter(col => rawStats.numericColumns.includes(col))
      : tableHeaders.filter(col => {
          const vals = tableRows.slice(0, 20).map(r => parseFloat(r[col])).filter(v => !isNaN(v));
          return vals.length > 5;
        });

    // Filter out ID-like and date-like columns
    return candidates.filter(col => !isIdCol(col) && !isDateCol(col));
  };


  const cNulls = rawStats.columnNulls || {};
  const totNulls = rawStats.totalNulls || 0;
  const nullCols = Object.keys(cNulls).filter(c => cNulls[c] > 0);
  const totDupes = rawStats.totalDuplicates || 0;
  const numCols = getNumCols();


  const syncPreview = async () => {
    if (!dsId) return false;

    const previewRes = await getDatasetPreview(dsId, 1);
    if (!previewRes.success) {
      return false;
    }

    setPage(1);
    setTableRows(previewRes.data || []);
    setTableHeaders(previewRes.data?.length > 0 ? Object.keys(previewRes.data[0]) : []);
    setTotalRows(previewRes.totalRows || 0);
    if (previewRes.currentStats || previewRes.rawStats) {
      setRawStats(previewRes.currentStats || previewRes.rawStats);
    }
    return true;
  };

  const activeData = tableRows;
  
  const acceptedFeatObj = aiSuggestions.filter(s => featStatuses[s.id] === 'accept');
  const acceptedFeaturesPersisted =
    acceptedFeatObj.length > 0 && acceptedFeatObj.every((feat) => tableHeaders.includes(feat.col));
  
  // Logical Filtering for headers
  let showHeaders = [...tableHeaders];
  
  if (colFilter === 'highlighted') {
    if (currentStep === 1) {
      showHeaders = tableHeaders.filter(c => (rawStats.columnNulls?.[c] || 0) > 0);
    } else if (currentStep === 4) {
      // Outliers: use numeric columns that were flagged (this is a simplified check)
      showHeaders = getNumCols();
    }
    // For other steps, we show all as they affect rows or are global
  }
  
  // Deduplicated: accepted feature cols already in the dataset must not appear twice
  const limitedHeaders = Array.from(new Set([
    ...(showHeaders.length > 0 ? showHeaders.slice(0, 15) : []),
    ...acceptedFeatObj.map(f => f.col)
  ]));
  const finalHeaders = Array.from(
    new Set([
      ...showHeaders,
      ...acceptedFeatObj.map(f => f.col)
    ])
  );

  const getBadge = () => {
    switch (currentStep) {
      case 1: return { cls: 'clean-null-badge', txt: '● Nulls highlighted' };
      case 2: return { cls: 'clean-dupe-badge', txt: '● Dupes highlighted' };
      case 3: return { cls: 'clean-type-badge', txt: '● Type fixes highlighted' };
      case 4: return { cls: 'clean-outlier-badge', txt: '● Outliers highlighted' };
      case 5: return { cls: 'clean-feat-badge', txt: '✦ New features highlighted' };
      default: return { cls: '', txt: '' };
    }
  };

  const handleDownload = async () => {
    if (!dsId) return;
    try {
      if (acceptedFeatObj.length > 0 && !acceptedFeaturesPersisted) {
        setLoading(true);
        const transformConfig = { type: 'feature_eng', params: { features: acceptedFeatObj } };
        const previewRes = await previewCleaningStep(dsId, 5, transformConfig, false);
        if (!previewRes.success) throw new Error(previewRes.message || 'Failed to preview features before download.');
        const applyRes = await applyCleaningStep(dsId, 5);
        if (!applyRes.success) throw new Error(applyRes.message || 'Failed to apply features before download.');
        await syncPreview();
      }
      await downloadDataset(dsId, `${dsName}_cleaned.csv`);
    } catch (err) {
      setError(err.message || 'Failed to download cleaned CSV.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="clean-root">
      {/* Top Nav */}
      <div className="clean-topnav">
        <button className="clean-back-btn" onClick={() => navigate('/employee/datasets')}>← Back</button>
        <div>
          <div className="clean-ds-label">{dsName}</div>
          <div className="clean-ds-sublabel">
            v1 · {(rawStats.totalRows || tableRows.length).toLocaleString()} rows · {tableHeaders.length} cols · {cleaningState?.steps?.[STEPS[currentStep-1]?.statusKey] === 'committed' ? 'Done' : 'In Progress'}
          </div>
        </div>
        <div className="clean-topnav-right">
          <div style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 10, color: 'var(--amber)', display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--amber)', display: 'inline-block', animation: 'cleanBlink 1s infinite' }}></span>
            {cleaningState?.steps?.[STEPS[currentStep-1]?.statusKey] === 'committed' ? 'Committed' : 'Cleaning in progress'}
          </div>
          <button className="clean-btn clean-btn-ghost clean-btn-sm" onClick={() => setVerifyOpen(true)}>Verify Dataset</button>
        </div>
      </div>

      {/* Timeline */}
      <div className="clean-timeline">
        {STEPS.map((s) => {
          const stepStatus = cleaningState?.steps?.[s.statusKey] || 'pending';
          let cls = 'clean-step';
          if (stepStatus === 'committed') cls += ' done';
          else if (s.id === currentStep) cls += ' active';
          if (s.id === 5) cls += ' feat';

          let statusText = 'Pending';
          if (stepStatus === 'committed') statusText = 'Done';
          else if (stepStatus === 'previewed') statusText = 'Previewing';
          else if (s.id === currentStep) {
             if (s.id === 1) statusText = `${totNulls} found`;
             if (s.id === 2) statusText = `${totDupes} dupes`;
             else statusText = 'Active';
          }

          return (
            <div key={s.id} className={cls} style={{ cursor: 'default' }}>
              <div className="clean-step-circle">{stepStatus === 'committed' ? '✓' : (s.id === 5 ? '✦' : s.id)}</div>
              <div className="clean-step-name">{s.shortName}</div>
              <div className="clean-step-status">{statusText}</div>
            </div>
          );
        })}
      </div>

      {/* Split View */}
      <div className="clean-split-wrap">
        
        {/* LEFT PANEL - Table */}
        <div className="clean-panel-left" style={{ width: `${leftWidth}%` }}>
          <div className="clean-panel-toolbar" style={{ justifyContent: 'space-between' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div className="clean-toolbar-label">Showing <strong>all {showHeaders.length} columns</strong></div>
              <div className={`clean-step-badge ${getBadge().cls}`} style={{ marginLeft: 0 }}>{getBadge().txt}</div>
            </div>
            
            {/* Pagination Controls */}
            {totalRows > 0 && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, background: 'rgba(255,255,255,0.05)', padding: '4px 12px', borderRadius: 20, border: '1px solid rgba(255,255,255,0.1)' }}>
                <span style={{ fontSize: 12, color: 'var(--ink3)', fontFamily: "'IBM Plex Mono', monospace" }}>
                  {(page - 1) * 50 + 1}-{Math.min(page * 50, totalRows)} of {totalRows.toLocaleString()}
                </span>
                <div style={{ display: 'flex', gap: 4 }}>
                  <button 
                    disabled={page === 1 || loading} 
                    onClick={() => setPage(p => p - 1)}
                    style={{ background: 'none', border: 'none', color: page === 1 ? '#555' : '#aaa', cursor: page === 1 ? 'default' : 'pointer', fontSize: 16, padding: '0 4px' }}
                  >
                    ←
                  </button>
                  <button 
                    disabled={page * 50 >= totalRows || loading} 
                    onClick={() => setPage(p => p + 1)}
                    style={{ background: 'none', border: 'none', color: page * 50 >= totalRows ? '#555' : '#aaa', cursor: page * 50 >= totalRows ? 'default' : 'pointer', fontSize: 16, padding: '0 4px' }}
                  >
                    →
                  </button>
                </div>
              </div>
            )}

            <select 
              className="clean-col-select" 
              value={colFilter} 
              onChange={(e) => setColFilter(e.target.value)}
            >
              <option value="all">All Columns</option>
              <option value="highlighted">Affected Columns Only</option>
            </select>
          </div>
          <div className="clean-data-scroll">
            {loading ? (
              <div style={{ padding: 40, textAlign: 'center', color: 'var(--ink3)' }}>Loading data...</div>
            ) : error ? (
              <div style={{ padding: 60, textAlign: 'center', color: 'var(--red)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16, marginTop: 40 }}>
                <div style={{ fontSize: 18, color: 'var(--red)' }}>Error Loading Dataset</div>
                <div style={{ fontSize: 14 }}>{error}</div>
                <button 
                  className="clean-btn clean-btn-ghost" 
                  onClick={() => navigate('/employee/datasets')}
                  style={{ marginTop: 8 }}
                >
                  Go Back
                </button>
              </div>
            ) : tableRows.length === 0 ? (
              <div style={{ padding: 60, textAlign: 'center', color: 'var(--ink3)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16, marginTop: 40 }}>
                <div style={{ fontSize: 18, color: 'var(--ink)' }}>No Dataset Selected</div>
                <div style={{ fontSize: 12 }}>You haven&apos;t selected a dataset to clean yet. Please choose one to get started.</div>
                <button 
                  className="clean-btn clean-btn-primary" 
                  onClick={() => navigate('/employee/datasets')}
                  style={{ marginTop: 8 }}
                >
                  Go to Datasets Page →
                </button>
              </div>
            ) : (
              <table className="clean-data-table">
                <thead>
                  <tr>
                    <th>#</th>
                    {limitedHeaders.map(col => {
                      const isNew = acceptedFeatObj.some(f => f.col === col);
                      const isNullCol = (rawStats.columnNulls?.[col] || 0) > 0;
                      const thCls = `${isNew ? 'col-new' : ''} ${isNullCol && currentStep === 1 ? 'col-problem' : ''}`;
                      return <th key={col} className={thCls}>{col}</th>;
                    })}
                  </tr>
                </thead>
                <tbody>
                  {activeData.map((row, ri) => (
                    <tr key={ri}>
                      <td className="row-num">{(page - 1) * 50 + ri + 1}</td>
                      {limitedHeaders.map(col => {
                        const val = row[col];
                        const isNull = val == null || String(val).trim() === '';
                        let cls = '';
                        if (currentStep === 1) {
                          if (isNull) cls = 'cell-null';
                          else if (!isNull && settings[1][col] && settings[1][col] !== 'Keep as-is') cls = 'cell-filled';
                        }
                        const isNew = acceptedFeatObj.some(f => f.col === col);
                        if (isNew) cls = 'cell-new';
                        return <td key={col} className={cls}>{isNull && currentStep === 1 ? (colFilter === 'highlighted' ? 'NULL' : 'NULL') : String(val ?? '')}</td>;
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {/* Drag Handle */}
        <div className={`clean-drag-handle ${dragging ? 'dragging' : ''}`} style={{ left: `${leftWidth}%` }} onMouseDown={() => setDragging(true)}></div>

        {/* RIGHT PANEL - Step Control */}
        <div className="clean-panel-right">
          <div className="clean-rpanel-head">
            <div className="clean-rpanel-title">Step {currentStep} — {STEPS[currentStep-1].name}</div>
            <div className="clean-rpanel-sub">Configure how you&apos;d like to clean this dataset</div>
          </div>
          <div className="clean-rpanel-body">
            
            {/* Step 1 */}
            {currentStep === 1 && (
              <div>
                <div className="clean-stat-row">
                  <div className="clean-stat-mini"><div className="clean-stat-mini-val" style={{color:'var(--red)'}}>{totNulls}</div><div className="clean-stat-mini-lbl">Nulls Found</div></div>
                  <div className="clean-stat-mini"><div className="clean-stat-mini-val">{nullCols.length}</div><div className="clean-stat-mini-lbl">Cols Affected</div></div>
                </div>
                <div className="clean-step-card">
                  <div className="clean-step-card-title">Column Strategies</div>
                  {nullCols.length === 0 ? (
                    <div style={{fontSize:11, color:'var(--green)'}}>No nulls found!</div>
                  ) : (
                    nullCols.map(col => (
                      <div className="clean-col-row" key={col}>
                        <div><div className="clean-col-name">{col}</div><div style={{fontFamily:"'IBM Plex Mono',monospace", fontSize:9, color:'var(--ink3)'}}>{cNulls[col]} nulls</div></div>
                        <div className="clean-col-stat danger">{cNulls[col]} nulls</div>
                        <select className="clean-strategy-sel" value={settings[1][col] || 'Keep as-is'} onChange={e => setSettings({...settings, 1: {...settings[1], [col]: e.target.value}})}>
                          {NULL_STRATEGIES.map(s => <option key={s}>{s}</option>)}
                        </select>
                      </div>
                    ))
                  )}
                </div>
              </div>
            )}

            {/* Step 2 */}
            {currentStep === 2 && (
              <div>
                <div className="clean-stat-row">
                  <div className="clean-stat-mini"><div className="clean-stat-mini-val" style={{color:'var(--amber)'}}>{totDupes}</div><div className="clean-stat-mini-lbl">Dupes Found</div></div>
                  <div className="clean-stat-mini"><div className="clean-stat-mini-val">{((rawStats.totalRows || tableRows.length) - totDupes).toLocaleString()}</div><div className="clean-stat-mini-lbl">Unique Rows</div></div>
                </div>
                <div className="clean-step-card">
                  <div className="clean-step-card-title">Duplicate Strategy</div>
                  <div style={{display:'flex', flexDirection:'column', gap:8, marginTop:4}}>
                    {DUPE_STRATEGIES.map(st => {
                      const isActive = settings[2].strategy === st.id;
                      return (
                        <label key={st.id} style={{ display:'flex', alignItems:'center', gap:10, cursor:'pointer', padding:'8px 10px', borderRadius:8, border:`1px solid ${isActive?'var(--accent)':'var(--border)'}`, background:isActive?'var(--accentbg)':'transparent' }}>
                          <input type="radio" name="dupstrat" checked={isActive} onChange={() => setSettings({...settings, 2:{strategy:st.id}})} style={{accentColor:'var(--accent)'}} />
                          <div><div style={{fontSize:12, fontWeight:500, color:'var(--ink)'}}>{st.label}</div><div style={{fontFamily:"'IBM Plex Mono',monospace", fontSize:9, color:'var(--ink3)', marginTop:2}}>{st.desc}</div></div>
                        </label>
                      );
                    })}
                  </div>
                </div>
              </div>
            )}

            {/* Step 3 */}
            {currentStep === 3 && (
              <div>
                <div className="clean-step-card">
                  <div className="clean-step-card-title">Type Adjustments</div>
                  {showHeaders.map(col => {
                    return (
                      <div className="clean-col-row" key={col}>
                        <div><div className="clean-col-name">{col}</div></div>
                        <select className="clean-strategy-sel" value={settings[3][col] || 'Auto-detect'} onChange={e => setSettings({...settings, 3: {...settings[3], [col]: e.target.value}})}>
                          {TYPE_STRATEGIES.map(s => <option key={s}>{s}</option>)}
                        </select>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Step 4 */}
            {currentStep === 4 && (
              <div>
                <div className="clean-step-card">
                  <div className="clean-step-card-title">Outlier Detect - Numeric Only</div>
                  {numCols.length === 0 ? (
                    <div style={{fontSize:11, color:'var(--ink3)'}}>No numeric columns detected.</div>
                  ) : (
                    numCols.map(col => (
                      <div className="clean-col-row" key={col}>
                        <div><div className="clean-col-name">{col}</div></div>
                        <select className="clean-strategy-sel" value={settings[4][col] || 'Keep as-is'} onChange={e => setSettings({...settings, 4: {...settings[4], [col]: e.target.value}})}>
                          {OUTLIER_STRATEGIES.map(s => <option key={s}>{s}</option>)}
                        </select>
                      </div>
                    ))
                  )}
                </div>
              </div>
            )}

            {/* Step 5 */}
            {currentStep === 5 && (
              <div>
                {featStreaming && (
                  <div className="feat-loading">
                    <div className="feat-loading-top">
                      <div className="feat-spinner"></div>
                      <div style={{fontFamily:"'IBM Plex Mono',monospace", fontSize:10, color:'var(--purple)'}}>Ollama analyzing dataset schema…</div>
                    </div>
                    <div className="feat-stream">
                      {featStreamText.split('\n').map((line, i) => <div key={i}>{line}</div>)}
                      <span className="clean-cursor-blink"></span>
                    </div>
                  </div>
                )}

                {featDone && (
                  <div>
                    <div style={{fontFamily:"'IBM Plex Mono',monospace", fontSize:9, color:'var(--ink3)', marginBottom:12, padding:'8px 10px', background:'var(--purplebg)', borderRadius:7, border:'1px solid rgba(167,139,250,0.15)'}}>
                      ✦ Ollama suggested {aiSuggestions.length} new features · Accept or reject each individually
                    </div>
                    {aiSuggestions.map(s => {
                      const status = featStatuses[s.id];
                      return (
                        <div className="feat-card" key={s.id} style={{ borderColor: status==='accept'?'rgba(34,211,238,0.5)':'rgba(34,211,238,0.18)', opacity: status==='reject'?0.4:1 }}>
                          <div className="feat-card-header">
                            <span className="feat-col-name">{s.col}</span>
                            <span className="feat-type-tag">{s.type}</span>
                          </div>
                          <div className="feat-formula"><strong>{s.col}</strong> = {s.formula}</div>
                          <div className="feat-desc">{s.desc}</div>
                          <div className="feat-actions">
                            {status !== 'reject' && (
                              <button className="feat-btn feat-accept" style={{ background: status==='accept'?'rgba(34,211,238,0.2)':'var(--tealbg)' }} onClick={() => setFeatStatuses({...featStatuses, [s.id]:'accept'})}>
                                {status === 'accept' ? '✓ Accepted' : '✓ Accept'}
                              </button>
                            )}
                            {status !== 'accept' && (
                              <button className="feat-btn feat-reject" style={{ background: status==='reject'?'rgba(251,113,133,0.2)':'var(--redbg)' }} onClick={() => setFeatStatuses({...featStatuses, [s.id]:'reject'})}>
                                {status === 'reject' ? '✕ Rejected' : '✕ Reject'}
                              </button>
                            )}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}

                {!featStreaming && !featDone && (
                  <button className="clean-btn clean-btn-ghost clean-skip-btn" style={{width:'100%', color:'var(--purple)', borderColor:'rgba(167,139,250,0.3)'}} onClick={startFeatStream}>
                    ✦ Start AI Feature Engineering
                  </button>
                )}

                <div className="clean-skip-bar" style={{ display: featDone ? 'flex' : 'none' }}>
                  <button className="clean-btn clean-btn-ghost clean-skip-btn">Skip Feature Eng.</button>
                  <button className="clean-btn clean-btn-green clean-skip-btn" onClick={() => setVerifyOpen(true)}>Finalize Dataset →</button>
                </div>
              </div>
            )}

            <div style={{ marginTop: 32, padding: '16px 0 0 0', borderTop: '1px solid var(--border)', display: 'flex', flexDirection: 'column', gap: 10 }}>
              {/* Previous button — always in same row as main actions */}
              {currentStep < 5 ? (
                <>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button
                      className="clean-btn"
                      style={{ flex: 1, background: 'rgba(167, 139, 250, 0.1)', color: 'var(--purple)', border: '1px solid rgba(167, 139, 250, 0.3)' }}
                      onClick={handleLetAiDecide}
                      disabled={loading}
                    >
                      ✦ Let AI Decide
                    </button>
                    <button
                      className="clean-btn clean-btn-primary"
                      style={{ flex: 1 }}
                      onClick={handleApplyFinal}
                      disabled={loading}
                    >
                      Apply Final Changes
                    </button>
                  </div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    {currentStep > 1 && (
                      <button
                        className="clean-btn clean-btn-ghost"
                        style={{ flex: 1 }}
                        onClick={() => setCurrentStep(prev => prev - 1)}
                        disabled={loading}
                      >
                        ← Previous Step
                      </button>
                    )}
                    <button
                      className="clean-btn clean-btn-ghost"
                      style={{ flex: 1, fontSize: 12, opacity: 0.8 }}
                      onClick={handleSkip}
                      disabled={loading}
                    >
                      Skip this step
                    </button>
                  </div>
                </>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button
                      className="clean-btn clean-btn-ghost"
                      style={{ flex: 1 }}
                      onClick={() => setCurrentStep(prev => Math.max(1, prev - 1))}
                      disabled={loading}
                    >
                      ← Previous Step
                    </button>
                    <button
                      className="clean-btn clean-btn-primary"
                      style={{ flex: 1 }}
                      disabled={loading}
                      onClick={async () => {
                        suppressLivePreview.current = true;
                        await handleApplyFinal();
                        suppressLivePreview.current = false;
                        setVerifyOpen(true);
                      }}
                    >
                      {loading ? 'Applying...' : 'Apply & Finalize →'}
                    </button>
                  </div>
                  <button
                    className="clean-btn clean-btn-ghost"
                    style={{ width: '100%', fontSize: 12, opacity: 0.8 }}
                    onClick={handleSkip}
                    disabled={loading}
                  >
                    Skip Feature Eng.
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>



      {/* VERIFY MODAL */}
      {verifyOpen && (
        <div className="clean-verify-overlay show" onClick={() => { setVerifyOpen(false); setShowFullCleaned(false); }}>
          <div className="clean-verify-modal" onClick={e => e.stopPropagation()}>
            <div className="clean-verify-head">
              <div>
                <div className="clean-verify-title">Verify Cleaned Dataset</div>
                <div style={{fontFamily:"'IBM Plex Mono',monospace", fontSize:10, color:'var(--ink3)', marginTop:2}}>{dsName} · v1 → cleaned · {showFullCleaned ? 'All Rows' : 'Preview: 50 rows'}</div>
              </div>
              <button className="clean-btn clean-btn-ghost clean-btn-sm" onClick={() => { setVerifyOpen(false); setShowFullCleaned(false); }}>✕ Close</button>
            </div>
            
            <div className="clean-verify-stats">
              <div className="clean-vstat"><div className="clean-vstat-val" style={{color:'var(--green)'}}>{(rawStats.totalRows || tableRows.length).toLocaleString()}</div><div className="clean-vstat-lbl">Rows After Cleaning</div></div>
              <div className="clean-vstat"><div className="clean-vstat-val" style={{color:'var(--red)'}}>{totNulls}</div><div className="clean-vstat-lbl">Nulls Handled</div></div>
              <div className="clean-vstat"><div className="clean-vstat-val" style={{color:'var(--amber)'}}>{totDupes}</div><div className="clean-vstat-lbl">Dupes Handled</div></div>
            </div>
            
            <div className="clean-verify-body">
              <table className="clean-data-table">
                <thead>
                  <tr>
                    <th>#</th>
                    {finalHeaders.map(col => {
                      const isNew = acceptedFeatObj.some(f => f.col === col);
                      return <th key={col} className={isNew ? 'col-new' : ''}>{col}</th>;
                    })}
                  </tr>
                </thead>
                <tbody>
                  {(showFullCleaned ? activeData : activeData.slice(0, 50)).map((row, ri) => (
                    <tr key={ri}>
                      <td className="row-num">{ri + 1}</td>
                      {finalHeaders.map(col => {
                         const isNew = acceptedFeatObj.some(f => f.col === col);
                         return <td key={col} className={isNew ? 'cell-new' : ''}>{String(row[col] ?? '')}</td>;
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            
            <div className="clean-verify-foot" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <button className="clean-btn clean-btn-ghost" onClick={() => { setVerifyOpen(false); setShowFullCleaned(false); }}>← Re-clean</button>
                <span style={{fontFamily:"'IBM Plex Mono',monospace", fontSize:10, color:'var(--ink3)', marginLeft:12}}>
                  {!showFullCleaned ? (
                    <>Showing preview · <span style={{color:'var(--accent2)', cursor:'pointer'}} onClick={() => setShowFullCleaned(true)}>View Full Dataset ↗</span></>
                  ) : (
                    <>Showing full set · <span style={{color:'var(--accent2)', cursor:'pointer'}} onClick={() => setShowFullCleaned(false)}>Collapse ↙</span></>
                  )}
                </span>
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button className="clean-btn clean-btn-primary" onClick={handleDownload}>↓ Download Cleaned CSV</button>
                <button 
                  className="clean-btn clean-btn-green" 
                  disabled={loading}
                  onClick={async () => {
                    setLoading(true);
                    try {
                      // 3. Move file to finalized (cleaned) directory
                      await finalizeDataset(dsId);
                      navigate(`/employee/visualization?ds=${dsId}&name=${encodeURIComponent(dsName)}`);
                    } catch (err) {
                      setError(err.message || "Failed to finalize dataset.");
                    } finally {
                      setLoading(false);
                    }
                  }}
                >
                  {loading ? 'Finalizing...' : 'Proceed to Visualization →'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default EmployeeCleaningPage;
