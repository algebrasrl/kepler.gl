const QMAP_LOCALE_MESSAGES = {
  it: {
    sidebar: {
      panels: {
        layer: 'Layer',
        filter: 'Filtri',
        interaction: 'Interazioni',
        basemap: 'Mappa di base',
        profile: 'Profilo',
        operations: 'Operazioni'
      }
    },
    layerManager: {
      addData: 'Aggiungi dati'
    },
    toolbar: {
      exportImage: 'Esporta immagine',
      exportData: 'Esporta dati',
      exportMap: 'Esporta mappa',
      saveMap: 'Salva mappa',
      shareMapURL: 'Condividi URL mappa',
      radius: 'Raggio'
    },
    color: {
      opacity: 'Opacità'
    },
    modal: {
      title: {
        addDataToMap: 'Aggiungi dati alla mappa'
      },
      loadData: {
        addData: 'Aggiungi dati',
        upload: 'Carica file',
        tileset: 'Tileset',
        storage: 'Archivio cloud',
        remote: 'Carica mappa da URL'
      }
    },
    fileUploader: {
      message: 'Trascina qui i file',
      chromeMessage:
        '*Utenti Chrome: limite dimensione file 250MB, per file piu grandi prova Safari',
      disclaimer:
        '*kepler.gl e un applicazione client-side senza backend server. I dati restano sul tuo browser/macchina. ' +
        'Nessuna informazione o dato mappa viene inviato a server.',
      configUploadMessage:
        'Carica {fileFormatNames} o una mappa **Json** salvata. Leggi di piu sui [**formati supportati**]',
      browseFiles: 'sfoglia i file',
      uploading: 'Caricamento',
      fileNotSupported: 'Il file {errorFiles} non e supportato.',
      or: 'oppure'
    },
    layer: {
      newLayer: 'nuovo layer',
      type: {h3: 'H3'}
    },
    mapStyle: {title: 'Stile mappa'},
    LayerBlending: {Title: 'Fusione layer'},
    OverlayBlending: {Title: 'Fusione overlay'},
    mapLegend: {
      layers: {
        wms: {singleColor: {color: 'Colore riempimento', strokeColor: 'Contorno'}}
      }
    },
    'Sync Zoom': 'Sincronizza zoom',
    'Unlock Viewport': 'Sblocca viewport',
    'Update color': 'Aggiorna colore',
    'Fill Opacity': 'Opacita riempimento',
    'Stroke Opacity': 'Opacita bordo',
    // AI Assistant panel
    tooltip: {
      showAiAssistantPanel: 'Mostra pannello AI Assistant',
      hideAiAssistantPanel: 'Nascondi pannello AI Assistant'
    },
    aiAssistantManager: {
      title: 'AI Assistant',
      aiProvider: 'Provider AI',
      llmModel: {title: 'Seleziona modello LLM'},
      apiKey: {title: 'Chiave API', placeholder: 'Inserisci la tua chiave API'},
      baseUrl: {title: 'Base URL', placeholder: 'Inserisci Base URL'},
      temperature: {title: 'Temperatura'},
      topP: {title: 'Top P'},
      startChat: 'Avvia chat'
    },
    loadRemoteMap: {
      description: 'Carica la mappa o dataset tramite URL',
      message: 'Incolla un URL raggiungibile dal browser.',
      examples: 'Esempi:',
      fetch: 'Carica'
    },
    fieldSelector: {
      clearAll: 'Cancella'
    },
    qmapAi: {
      title: 'Assistente',
      send: 'Invia',
      close: 'Chiudi',
      clear: 'Cancella chat',
      tooltip: 'Assistente AI'
    },
    qmapMode: {
      tooltip: 'Modalita mappa',
      kepler: 'Kepler',
      drawStressor: 'Draw Stressor',
      drawOnMap: 'Draw On Map',
      geotoken: 'Geotoken'
    },
    qmapHexPaint: {
      tooltip: 'Disegna esagoni H3'
    },
    qmapDrawTools: {
      drawPerimeter: 'Draw Perimeter',
      drawOperations: 'Draw Operations',
      point: 'Punto',
      line: 'Linea',
      polygon: 'Poligono',
      rectangle: 'Rettangolo',
      radius: 'Raggio'
    }
  },
  en: {
    sidebar: {
      panels: {
        profile: 'Profile',
        operations: 'Operations'
      }
    },
    modal: {
      loadData: {
        remote: 'Load Map using URL'
      }
    },
    loadRemoteMap: {
      description: 'Load map or dataset using URL',
      message: 'Paste a URL reachable by the browser.',
      examples: 'Examples:',
      fetch: 'Fetch'
    },
    fieldSelector: {
      clearAll: 'Clear'
    },
    qmapAi: {
      title: 'Assistant',
      send: 'Send',
      close: 'Close',
      clear: 'Clear chat',
      tooltip: 'AI Assistant'
    },
    qmapMode: {
      tooltip: 'Map mode',
      kepler: 'Kepler',
      drawStressor: 'Draw Stressor',
      drawOnMap: 'Draw On Map',
      geotoken: 'Geotoken'
    },
    qmapHexPaint: {
      tooltip: 'Draw H3 hexagons'
    },
    qmapDrawTools: {
      drawPerimeter: 'Draw Perimeter',
      drawOperations: 'Draw Operations',
      point: 'Point',
      line: 'Line',
      polygon: 'Polygon',
      rectangle: 'Rectangle',
      radius: 'Radius'
    }
  }
};

export default QMAP_LOCALE_MESSAGES;
