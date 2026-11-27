/**
 * Copia texto. El portapapeles moderno solo existe en HTTPS o localhost; por http://IP se usa la vía
 * antigua sobre el campo que lo enseña (`campo`), que queda seleccionado, o, sin campo, sobre uno
 * temporal fuera de pantalla. Devuelve si lo copió: si no, el texto del campo sigue seleccionado
 * para copiarlo con el teclado.
 */
export async function copyText(text: string, campo?: HTMLInputElement | HTMLTextAreaElement | null): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) await navigator.clipboard.writeText(text)
    else if (campo) {
      campo.select()
      if (!document.execCommand('copy')) throw new Error('copy')
    } else if (!copiaTemporal(text)) throw new Error('copy')
    return true
  } catch {
    campo?.select()
    return false
  }
}

/** La vía antigua sin campo a la vista: uno temporal, y el foco vuelve a donde estaba. */
function copiaTemporal(text: string): boolean {
  const antes = document.activeElement as HTMLElement | null
  const temporal = document.createElement('textarea')
  temporal.value = text
  temporal.readOnly = true // que no saque el teclado en el móvil
  temporal.style.cssText = 'position:fixed;top:0;left:-9999px;opacity:0'
  document.body.appendChild(temporal)
  try {
    temporal.select()
    temporal.setSelectionRange(0, text.length) // iOS no hace caso a select()
    return document.execCommand('copy')
  } finally {
    temporal.remove()
    antes?.focus?.()
  }
}
