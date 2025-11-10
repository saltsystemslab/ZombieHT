DOC=main
OPTS = -pdf
LATEX = pdflatex

default:
	@if (command -v latexmk > /dev/null) ; then latexmk $(OPTS) $(DOC) ; else make brute; fi 

cont: 
	latexmk -pvc $(OPTS) $(DOC)

brute-nonstop:
	$(LATEX) -pdf -interaction=nonstopmode -f $(DOC)
	bibtex $(DOC)
	$(LATEX) -pdf -interaction=nonstopmode -f $(DOC)
	$(LATEX) -pdf -interaction=nonstopmode -f $(DOC)

brute:
	$(LATEX) $(DOC)
	bibtex $(DOC)
	$(LATEX) $(DOC)
	$(LATEX) $(DOC)

clean:
	latexmk -C
